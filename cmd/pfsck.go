package cmd

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"

	"github.com/urfave/cli/v2"
)

func cmdPFsck() *cli.Command {
	return &cli.Command{
		Name:      "pfsck",
		Action:    pfsck,
		Category:  "ADMIN",
		Usage:     "Check consistency of a volume",
		ArgsUsage: "META-URL",
		Description: `
It scans all objects in data storage and slices in metadata, comparing them to see if there is any
lost object or broken file.

Examples:
$ juicefs fsck redis://localhost

# Repair broken directories
$ juicefs fsck redis://localhost --path /d1/d2 --repair

# recursively check
$ juicefs fsck redis://localhost --path /d1/d2 --recursive`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "path",
				Usage: "absolute path within JuiceFS to check",
				Value: "/",
			},
			&cli.BoolFlag{
				Name:  "repair",
				Usage: "repair specified path if it's broken",
			},
			&cli.IntFlag{
				Name:    "concurrent",
				Aliases: []string{"c"},
				Usage:   "check concurrent",
				Value:   2,
			},
			&cli.StringFlag{
				Name:    "check_time",
				Aliases: []string{"ct"},
				Usage:   "check time",
				Value:   "2024-10-01",
			},
			&cli.StringFlag{
				Name:    "fail_record",
				Aliases: []string{"fr"},
				Usage:   "fail record save path",
			},
			&cli.BoolFlag{
				Name:  "sync-dir-stat",
				Usage: "sync stat of all directories, even if they are existed and not broken (NOTE: it may take a long time for huge trees)",
			},
		},
	}
}

type Ino = meta.Ino

type _file struct {
	ino  Ino
	Name string
	size uint64
	ss   []meta.Slice
}

type Attr = meta.Attr

func walkDir(ctx meta.Context, m meta.Meta, inode Ino, todo chan _file, time int64) {
	pending := make([]Ino, 1)
	pending[0] = inode
	for len(pending) > 0 {
		l := len(pending)
		l--
		inode = pending[l]
		pending = pending[:l]
		var entries []*meta.Entry
		r := m.Readdir(ctx, inode, 1, &entries)
		if r == 0 {
			for _, f := range entries {
				name := string(f.Name)
				if name == "." || name == ".." {
					continue
				}

				if f.Attr.Typ == meta.TypeDirectory {
					pending = append(pending, f.Inode)
				} else if f.Attr.Typ != meta.TypeSymlink {
					var slices []meta.Slice
					m.ListSlicesByIno(ctx, f.Inode, &slices, false, func() {

					})
					if f.Attr.Ctime < time {
						continue
					}
					todo <- _file{f.Inode, name, f.Attr.Length, slices}
				}
				if ctx.Canceled() {
					return
				}
			}
		} else {
			logger.Warnf("readdir %d: %s", inode, r)
		}
	}

	close(todo)
}

func pfsck(ctx *cli.Context) error {
	setup(ctx, 1)
	if ctx.Bool("repair") && ctx.String("path") == "" {
		logger.Fatalf("Please provide the path to repair with `--path` option")
	}
	removePassword(ctx.Args().Get(0))
	m := meta.NewClient(ctx.Args().Get(0), nil)
	format, err := m.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	var c = meta.NewContext(0, 0, []uint32{0})

	chunkConf := chunk.Config{
		BlockSize:  format.BlockSize * 1024,
		Compress:   format.Compression,
		GetTimeout: time.Second * 60,
		PutTimeout: time.Second * 60,
		MaxUpload:  20,
		BufferSize: 300 << 20,
		CacheDir:   "memory",
	}

	blob, err := createStorage(*format)
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	logger.Infof("Data use %s", blob)
	blob = object.WithPrefix(blob, "chunks/")
	if err != nil {
		logger.Fatalf("list all blocks: %s", err)
	}

	// Find all blocks in object storage

	var inode Ino
	var attr = &Attr{}
	p := ctx.String("path")
	date_str := ctx.String("check_time")
	logger.Infof("scan check path:%s by concurrent:%d checktime:%s", p, ctx.Int("concurrent"), date_str)
	if errr := resolve(c, m, p, &inode, attr); errr != 0 {
		logger.Fatalf("error while resolve dir ....,%d", errr)
	}

	// 定义日期格式
	layout := "2006-01-02"

	// 解析日期字符串为 time.Time 对象
	parsedTime, err := time.Parse(layout, date_str)
	if err != nil {
		logger.Errorf("解析日期字符串时出错:%s", err)
	}

	// 将 time.Time 对象转换为 Unix 时间戳（秒数）
	unixTimestamp := parsedTime.Unix()
	logger.Infof("check time:%d", unixTimestamp)

	todo := make(chan _file, 10240)
	logger.Infof("scan inode:%d", inode)
	go func() {
		walkDir(c, m, inode, todo, unixTimestamp)
	}()

	// List all slices in metadata engine
	delfiles := make(map[meta.Ino]bool)
	err = m.ScanDeletedObject(c, nil, nil, nil, func(ino meta.Ino, size uint64, ts int64) (clean bool, err error) {
		delfiles[ino] = true
		return false, nil
	})
	if err != nil {
		logger.Warnf("scan deleted objects: %s", err)
	}

	// Scan all slices to find lost blocks
	brokens := make(map[meta.Ino]string)

	var wg sync.WaitGroup
	var count int32
	for i := 0; i < ctx.Int("concurrent"); i++ {
		wg.Add(1)
		go worker(chunkConf, todo, format, delfiles, m, brokens, blob, &wg, &count)
	}

	wg.Wait()
	logger.Infof("check file total num:%d", count)
	if len(brokens) > 0 {
		msg := fmt.Sprintf(" broken files:%d \n", len(brokens))
		var fileList []string
		for i, p := range brokens {
			fileList = append(fileList, fmt.Sprintf("%13d: %s", i, p))
		}
		sort.Strings(fileList)
		msg += strings.Join(fileList, "\n")

		fail_record := ctx.String("fail_record")
		logger.Infof("save fail record in path:%s", fail_record)
		file, err := os.OpenFile(fail_record, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			logger.Fatalf("Error opening file:%s", err)
		}
		defer file.Close()

		// 将字符串转换为字节切片
		data := []byte(msg)

		// 写入数据到文件
		_, err = file.Write(data)
		if err != nil {
			logger.Fatalf("Error writing to file:%s", err)
		}
	}
	return nil
}

func worker(chunkConf chunk.Config, todo chan _file, format *meta.Format, delfiles map[meta.Ino]bool, m meta.Meta, brokens map[meta.Ino]string, blob object.ObjectStorage, wg *sync.WaitGroup, count *int32) {

	var pnum int32
	pnum = 100
	defer wg.Done()
	for f := range todo {

		ss := f.ss
		inode := f.ino
		if f.ino == 0 {
			break
		}

		if delfiles[inode] {
			continue
		}
		atomic.AddInt32(count, 1)
		if *count%pnum == 0 {
			logger.Infof("check file num:%d", *count)
		}

		for _, s := range ss {
			n := (s.Size - 1) / uint32(chunkConf.BlockSize)
			for i := uint32(0); i <= n; i++ {
				sz := chunkConf.BlockSize
				if i == n {
					sz = int(s.Size) - int(i)*chunkConf.BlockSize
				}
				key := fmt.Sprintf("%d_%d_%d", s.Id, i, sz)
				var objKey string
				if format.HashPrefix {
					objKey = fmt.Sprintf("%02X/%v/%s", s.Id%256, s.Id/1000/1000, key)
				} else {
					objKey = fmt.Sprintf("%v/%v/%s", s.Id/1000/1000, s.Id/1000, key)
				}
				if _, err := blob.Head(objKey); err != nil {
					if _, ok := brokens[inode]; !ok {
						if ps := m.GetPaths(meta.Background, inode); len(ps) > 0 {
							brokens[inode] = ps[0]
						} else {
							brokens[inode] = fmt.Sprintf("inode:%d", inode)
						}
					}
					logger.Errorf("can't find block %s for file %s: %s", objKey, f.Name, err)
				}
			}
		}

	}
}

func resolve(ctx meta.Context, m meta.Meta, p string, inode *Ino, attr *Attr) syscall.Errno {
	var inodePrefix = "inode:"
	if strings.HasPrefix(p, inodePrefix) {
		i, err := strconv.ParseUint(p[len(inodePrefix):], 10, 64)
		if err == nil {
			*inode = meta.Ino(i)
			return m.GetAttr(ctx, meta.Ino(i), attr)
		}
	}
	p = strings.Trim(p, "/")
	err := m.Resolve(ctx, 1, p, inode, attr)
	if err != syscall.ENOTSUP {
		return err
	}

	// Fallback to the default implementation that calls `m.Lookup` for each directory along the path.
	// It might be slower for deep directories, but it works for every meta that implements `Lookup`.
	parent := Ino(1)
	ss := strings.Split(p, "/")
	for i, name := range ss {
		if len(name) == 0 {
			continue
		}
		if parent == meta.RootInode && i == len(ss)-1 && vfs.IsSpecialName(name) {
			*inode, attr = vfs.GetInternalNodeByName(name)
			parent = *inode
			break
		}
		if i > 0 {
			if err = m.Access(ctx, parent, vfs.MODE_MASK_R|vfs.MODE_MASK_X, attr); err != 0 {
				return err
			}
		}
		if err = m.Lookup(ctx, parent, name, inode, attr, false); err != 0 {
			return err
		}
		if attr.Typ == meta.TypeSymlink {
			var buf []byte
			if err = m.ReadLink(ctx, *inode, &buf); err != 0 {
				return err
			}
			target := string(buf)
			if strings.HasPrefix(target, "/") || strings.Contains(target, "://") {
				return syscall.ENOTSUP
			}
			target = path.Join(strings.Join(ss[:i], "/"), target)
			if err = resolve(ctx, m, target, inode, attr); err != 0 {
				return err
			}
		}
		parent = *inode
	}
	if parent == meta.RootInode {
		*inode = parent
		if err = m.GetAttr(ctx, *inode, attr); err != 0 {
			return err
		}
	}
	return 0
}