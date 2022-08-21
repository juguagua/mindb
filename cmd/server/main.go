package main

import (
	"flag"
	"fmt"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"log"
	"mindb"
	"mindb/cmd"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	// print banner
	banner, _ := ioutil.ReadFile("../../resource/banner.txt")
	fmt.Println(string(banner))
}

var config = flag.String("config", "", "the config file for mindb")
var dirPath = flag.String("dir_path", "", "the dir path for the database")

func main() {
	flag.Parse() // 解析配置

	//set the config
	var cfg mindb.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
		cfg = mindb.DefaultConfig()
	} else {
		c, err := newConfigFromFile(*config)
		if err != nil {
			log.Printf("load config err : %+v\n", err)
			return
		}
		cfg = *c
	}

	if *dirPath == "" {
		log.Println("no dir path set, using the os tmp dir.")
	} else {
		cfg.DirPath = *dirPath
	}

	//listen the server
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	server, err := cmd.NewServer(cfg)
	if err != nil {
		log.Printf("create mindb server err: %+v\n", err)
		return
	}
	go server.Listen(cfg.Addr)

	<-sig
	server.Stop()
	log.Println("mindb is ready to exit, bye...")
}

func newConfigFromFile(config string) (*mindb.Config, error) {
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	var cfg = new(mindb.Config)
	err = toml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
