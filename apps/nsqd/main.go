package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	// 阻塞方法，等待系统退出信号量，SIGTERM： kill -15 PID退出报错
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	// 设置命令行参数Options的默认值
	opts := nsqd.NewOptions()

	// 解析命令行参数，先设置默认值
	flagSet := nsqdFlagSet(opts)
	// 使用命令行传入的值覆盖默认值
	flagSet.Parse(os.Args[1:])

	// 使用当前时间戳作为随机数的种子，而不是伪随机
	rand.Seed(time.Now().UTC().UnixNano())
	// nsqd -version用于打印版本号，并退出
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	// 读取NSQD的服务配置文件，并放到cfg这个map中
	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)

	// 创建NSQD - 核心方法
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	return nil
}

func (p *program) Start() error {
	// 加载元数据 - Topic和channel的信息
	err := p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	// 持久化元数据
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	go func() {
		// 启动 nsqd， Main方法内部会阻塞，所以得开启一个新的goroutine
		err := p.nsqd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	//当前goroutine并不会阻塞
	return nil
}

func (p *program) Stop() error {
	// nsqd只会执行一次退出
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}

// Context returns a context that will be canceled when nsqd initiates the shutdown
func (p *program) Context() context.Context {
	return p.nsqd.Context()
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
