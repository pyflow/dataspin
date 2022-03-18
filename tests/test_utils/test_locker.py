import datetime as dt
import gc
import os
import random
import string

import pytest

from dataspin.utils.locker import Locker, AcquireFailure

# Default URL will work for CI tests
db_url = os.environ.get('DB_URL', 'postgresql://postgres:postgres@localhost/postgres')


def random_str(length):
    return ''.join(random.choice(string.printable) for _ in range(25))


def test_lock_num_generation():
    locker = Locker('TestLocker', db_url)

    names = [random_str(max(6, x % 25)) for x in range(5000)]
    assert len(set(names)) == 5000
    nums = [locker._lock_num(name) for name in names]
    assert len(set(nums)) == 5000

def test_locker_defaults():
    lock = Locker('foo', db_url).lock('a')
    assert lock.blocking is True
    assert lock.acquire_timeout == 30000

    lock = Locker('bar', db_url, blocking_default=False, acquire_timeout_default=1000) \
        .lock('a')
    assert lock.blocking is False
    assert lock.acquire_timeout == 1000

def duration(started_at):
    duration = dt.datetime.now() - started_at
    secs = duration.total_seconds()
    # in milliseconds
    return secs * 1000

def test_same_lock_fails_acquire():
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock1 = locker.lock('test_it')
    lock2 = locker.lock('test_it')

    try:
        assert lock1.acquire() is True

        # Non blocking version should fail immediately.  Use `is` test to make sure we get the
        # correct return value.
        assert lock2.acquire(blocking=False) is False

        # Blocking version should fail after a short time
        start = dt.datetime.now()
        acquired = lock2.acquire(acquire_timeout=300)
        waited_ms = duration(start)

        assert acquired is False
        assert waited_ms >= 300 and waited_ms < 350
    finally:
        lock1.release()

def test_different_lock_name_both_acquire():
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock1 = locker.lock('test_it')
    lock2 = locker.lock('test_it2')

    try:
        assert lock1.acquire() is True
        assert lock2.acquire() is True
    finally:
        lock1.release()
        lock2.release()

def test_lock_after_release_acquires():
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock1 = locker.lock('test_it')
    lock2 = locker.lock('test_it')

    try:
        assert lock1.acquire() is True
        assert lock1.release() is True
        assert lock2.acquire() is True
    finally:
        lock1.release()
        lock2.release()

def test_class_params_used():
    """
        If blocking & timeout params are set on the class, make sure they are passed through and
        used correctly.
    """
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock1 = locker.lock('test_it')
    lock2 = locker.lock('test_it', blocking=False)
    lock3 = locker.lock('test_it', acquire_timeout=300)

    try:
        assert lock1.acquire() is True

        # Make sure the blocking param applies
        acquired = lock2.acquire()
        assert acquired is False

        # Make sure the retry params apply
        start = dt.datetime.now()
        acquired = lock3.acquire()
        waited_ms = duration(start)
        assert acquired is False
        assert waited_ms >= 300 and waited_ms < 350
    finally:
        lock1.release()
        lock2.release()
        lock3.release()

def test_shared_lock():
    """
        Test shared lock.
    """
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    shared_lock = locker.lock('test_it', shared=True)
    another_shared_lock = locker.lock('test_it', shared=True)
    exclusive_lock = locker.lock('test_it', shared=False, blocking=False)
    try:
        assert shared_lock.acquire() is True
        assert another_shared_lock.acquire() is True

        # Assert that an exclusive lock cannot be obtained
        assert exclusive_lock.acquire() is False

        # Release one of the shared locks.
        shared_lock.release()

        # Assert that the exclusive still cannot be obtained.
        assert exclusive_lock.acquire() is False

        # Release the other shared lock.
        another_shared_lock.release()

        # Assert that the exclusive lock can now be obtained.
        assert exclusive_lock.acquire() is True
    finally:
        shared_lock.release()
        another_shared_lock.release()
        exclusive_lock.release()

def test_context_manager():
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock2 = locker.lock('test_it', blocking=False)
    try:
        with locker.lock('test_it'):
            assert lock2.acquire() is False

        # Outside the lock should have been released and we can get it now
        assert lock2.acquire()
    finally:
        lock2.release()

def test_context_manager_failure_to_acquire():
    """
        By default, we want a context manager's failure to acquire to be a hard error so that
        a developer doesn't have to remember to explicilty check the return value of acquire
        when using a with statement.
    """
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock2 = locker.lock('test_it', blocking=False)
    assert lock2.acquire() is True

    with pytest.raises(AcquireFailure):
        with locker.lock('test_it'):
            pass  # we should never hit this line

def test_gc_lock_release():
    locker = Locker('TestLock', db_url, acquire_timeout_default=1000)
    lock1 = locker.lock('test_it')
    lock1.acquire()
    del lock1
    gc.collect()

    assert locker.lock('test_it', blocking=False).acquire()