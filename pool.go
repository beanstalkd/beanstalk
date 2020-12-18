package beanstalk

// github.com/garyburd/redigo/blob/master/redis/pool.go
import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing
var ErrPoolExhausted = errors.New("beanstalk: connection pool exhausted")

var (
	errPoolClosed = errors.New("beanstalk: connection pool closed")
	errConnClosed = errors.New("beanstalk: connection closed")
)

type IConn interface {
    Close() error
    Delete(uint64) error
    Release(uint64, uint32, time.Duration) error
    Bury(uint64, uint32) error
    Touch(uint64) error
    Peek(uint64) ([]byte, error)
    Stats() (map[string]string, error)
    StatsJob(uint64) (map[string]string, error)
    StatsTube(string) (map[string]string, error)
    ListTubes() ([]string, error)

    // Implemented in tube
    Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error)
    PeekReady() (id uint64, body []byte, err error)
    PeekDelayed() (id uint64, body []byte, err error)
    PeekBuried() (id uint64, body []byte, err error)
    Kick(bound int) (n int, err error)
    Pause(d time.Duration) error

    UseTube(name string) error
}

type Pool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	Dial func() (IConn, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c IConn, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c IConn
	t time.Time
}

// NewPool creates a new pool.
//
// Deprecated: Initialize the Pool directory as shown in the example.
func NewPool(newFn func() (IConn, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get() IConn {
	c, err := p.get()
	if err != nil {
		return errorConnection{err}
	}
	return &pooledConnection{p: p, c: c}
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *Pool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (IConn, error) {
	p.mu.Lock()

	// Prune stale connections.

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {

		// Get idle connection.

		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c, ic.t) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.

		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("beanstalk: get on closed pool")
		}

		// Dial new connection if under limit.

		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active += 1
			p.mu.Unlock()
			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *Pool) put(c IConn) error {
	p.mu.Lock()
	if !p.closed {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()
	return c.Close()
}

type pooledConnection struct {
	p     *Pool
	c     IConn
}

func (pc *pooledConnection) Close() error {
	c := pc.c
	if _, ok := c.(errorConnection); ok {
		return nil
	}
	pc.c = errorConnection{errConnClosed}

	pc.p.put(c)
	return nil
}

func (pc *pooledConnection) Delete(id uint64) error {
    return pc.c.Delete(id)
}

func (pc *pooledConnection) Release(id uint64, pri uint32, delay time.Duration) error {
    return pc.c.Release(id, pri, delay)
}

func (pc *pooledConnection) Bury(id uint64, pri uint32) error {
    return pc.c.Bury(id, pri)
}

func (pc *pooledConnection) Touch(id uint64) error {
    return pc.c.Touch(id)
}

func (pc *pooledConnection) Peek(id uint64) ([]byte, error) {
    return pc.c.Peek(id)
}

func (pc *pooledConnection) Stats() (map[string]string, error) {
    return pc.c.Stats()
}

func (pc *pooledConnection) StatsJob(id uint64) (map[string]string, error) {
    return pc.c.StatsJob(id)
}

func (pc *pooledConnection) StatsTube(tube string) (map[string]string, error) {
    c, ok := pc.c.(*Conn)
    if !ok {
        return nil, errors.New("pooledConnection conn cannot assert *Conn")
    }
    tb := &Tube{Conn: c, Name: tube}
    return tb.Stats()
}

func (pc *pooledConnection) ListTubes() ([]string, error) {
    return pc.c.ListTubes()
}

func (pc *pooledConnection) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
    return pc.c.Put(body, pri, delay, ttr)
}

func (pc *pooledConnection) PeekReady() (id uint64, body []byte, err error) {
    return pc.c.PeekReady()
}

func (pc *pooledConnection) PeekDelayed() (id uint64, body []byte, err error) {
    return pc.c.PeekDelayed()
}

func (pc *pooledConnection) PeekBuried() (id uint64, body []byte, err error) {
    return pc.c.PeekBuried()
}

func (pc *pooledConnection) Kick(bound int) (n int, err error) {
    return pc.c.Kick(bound)
}

func (pc *pooledConnection) Pause(d time.Duration) error {
    return pc.c.Pause(d)
}

func (pc *pooledConnection) UseTube(name string) error {
    c, ok := pc.c.(*Conn)
    if !ok {
        return errors.New("pooledConnection conn cannot assert *Conn")
    }

    c.UseTube(name)
    return nil
}

type errorConnection struct{ err error }

func (ec errorConnection) Close() (error) { return ec.err }
func (ec errorConnection) Delete(uint64) (error) { return ec.err }
func (ec errorConnection) Release(uint64, uint32, time.Duration) (error) { return ec.err }
func (ec errorConnection) Bury(uint64, uint32) (error) { return ec.err }
func (ec errorConnection) Touch(uint64) (error) { return ec.err }
func (ec errorConnection) Peek(uint64) ([]byte, error) { return nil, ec.err }
func (ec errorConnection) Stats() (map[string]string, error) { return nil, ec.err }
func (ec errorConnection) StatsJob(uint64) (map[string]string, error) { return nil, ec.err }
func (ec errorConnection) StatsTube(string) (map[string]string, error) { return nil, ec.err }
func (ec errorConnection) ListTubes() ([]string, error) { return nil, ec.err }
func (ec errorConnection) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {return 0, ec.err}
func (ec errorConnection) PeekReady() (id uint64, body []byte, err error) {return 0, nil, ec.err}
func (ec errorConnection) PeekDelayed() (id uint64, body []byte, err error) {return 0, nil, ec.err}
func (ec errorConnection) PeekBuried() (id uint64, body []byte, err error) {return 0, nil, ec.err}
func (ec errorConnection) Kick(bound int) (n int, err error) {return 0, ec.err}
func (ec errorConnection) Pause(d time.Duration) error {return ec.err}
func (ec errorConnection) UseTube(name string) error {return ec.err}
