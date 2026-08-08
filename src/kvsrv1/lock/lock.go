package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type LockStatus string

const (
	LockStatusLocked   LockStatus = "LOCKED"
	LockStatusUnlocked LockStatus = "UNLOCKED"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck          kvtest.IKVClerk
	lockname    string
	currStatus  LockStatus
	currVersion rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	return lk
}

func (lk *Lock) waitWhileLocked() {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		lk.currStatus = LockStatus(val)
		lk.currVersion = ver
		if err == rpc.OK && lk.currStatus == LockStatusLocked {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}

func (lk *Lock) Acquire() {
	for {
		err := lk.ck.Put(lk.lockname, string(LockStatusLocked), lk.currVersion)
		if err != rpc.OK {
			lk.waitWhileLocked()
			continue
		}
		break
	}
	lk.currStatus = LockStatusLocked
	lk.currVersion += 1
}

func (lk *Lock) Release() {
	err := lk.ck.Put(lk.lockname, string(LockStatusUnlocked), lk.currVersion)
	if err != rpc.OK {
		panic(err)
	}
}
