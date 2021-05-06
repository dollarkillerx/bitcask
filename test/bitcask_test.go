package test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dollarkillerx/bitcask"
)

func TestBitCask(t *testing.T) {
	log.SetFlags(log.Llongfile | log.LstdFlags)

	dir := "test_a"
	os.RemoveAll("test_a")
	cask, err := bitcask.New(dir, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer cask.Close()

	get, err := cask.Get([]byte("ppc"))
	if err != nil {
		log.Println(err)
	}
	log.Println("ppc: ", get)

	err = cask.Set([]byte("ppc"), []byte("ppc value"))
	if err != nil {
		log.Fatalln(err)
	}

	//time.Sleep(time.Second * 6)

	get, err = cask.Get([]byte("ppc"))
	if err != nil {
		log.Printf("%+v\n", err)
	}
	log.Println("2ppc: ", string(get))

	err = cask.Del([]byte("ppc"))
	if err != nil {
		log.Printf("%v\n", err)
	}

	get, err = cask.Get([]byte("ppc"))
	if err != nil {
		log.Printf("%+v\n", err)
	}
	log.Println("3ppc: ", string(get))

	err = cask.Del([]byte("ppc"))
	if err != nil {
		log.Printf("%v\n", err)
	}


	err = cask.Set([]byte("ppc"), []byte("ppc value"))
	if err != nil {
		log.Fatalln(err)
	}
}

func TestPressure(t *testing.T) {
	log.SetFlags(log.Llongfile | log.LstdFlags)

	dir := "test_pressure"
	cask, err := bitcask.New(dir, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer cask.Close()

	now := time.Now()
	for i:=0;i<10000000;i++ {
		err := cask.Set([]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("新德里电视台5日消息称，印度海军的三个司令部已经派出了9艘战舰，执行“海上桥梁Ⅱ”运输任务。先前派出的“塔瓦尔”号护卫舰已经从巴林返回印度卡纳塔克邦，带回了两个分别重达27吨的氧气罐。携带大量氧气供给的“加尔各答”号驱逐舰，也开始从科威特返印 this is value %d ", i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	fmt.Println("insert sec: ", time.Since(now).Seconds())
}

func TestSimple(t *testing.T) {
	log.SetFlags(log.Llongfile | log.LstdFlags)

	dir := "test_a"
	cask, err := bitcask.New(dir, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer cask.Close()

	get, err := cask.Get([]byte("ppc"))
	if err != nil {
		log.Println(err)
	}
	log.Println("ppc: ", get)
}
