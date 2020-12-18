package beanstalk

import (
	"testing"
	"time"
    "errors"
    "sync"
)

func TestGet(t *testing.T) {
    pool := &Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (IConn, error) {
            c := &Conn{}
            return c, nil
        },
        TestOnBorrow: func(c IConn, t time.Time) error {
            return nil
        },
    }

    conn := pool.Get()
    if _, ok := conn.(errorConnection); ok {
        err := errors.New("Cannot get conn via pool.")
		t.Fatal(err)
    }

    if _, ok := conn.(*pooledConnection); !ok {
        err := errors.New("Cannot get conn via pool.")
		t.Fatal(err)
    }

    if pool.ActiveCount() != 1 {
        err := errors.New("Pool acitve count invalid")
		t.Fatal(err)
    }

    conn.Close()

    if pool.ActiveCount() != 1 {
        err := errors.New("Pool acitve count invalid")
		t.Fatal(err)
    }
}

func TestConcurrentGet(t *testing.T) {
    pool := &Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (IConn, error) {
            c := &Conn{}
            return c, nil
        },
        TestOnBorrow: func(c IConn, t time.Time) error {
            return nil
        },
    }
    var wg sync.WaitGroup
    var i int
    go func() {
        wg.Add(1)
        for {
            if i > 20 {
                break
            }
            conn := pool.Get()
            conn.Close()

            time.Sleep(100 * time.Millisecond)
            i++
        }
        wg.Done()
    }()
    var j int
    go func() {
        wg.Add(1)
        for {
            if j > 30 {
                break
            }
            conn := pool.Get()
            conn.Close()

            time.Sleep(100 * time.Millisecond)
            j++
        }
        wg.Done()
    }()

    wg.Wait()
    if pool.ActiveCount() != 0 {
        err := errors.New("Pool acitve count invalid")
		t.Fatal(err)
    }
}

func TestClose(t *testing.T) {
    pool := &Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (IConn, error) {
            c := &Conn{}
            return c, nil
        },
        TestOnBorrow: func(c IConn, t time.Time) error {
            return nil
        },
    }

    err := pool.Close()
    if err != nil {
		t.Fatal(err)
    }
}
