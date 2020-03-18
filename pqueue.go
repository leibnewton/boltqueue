package boltqueue

import (
	"encoding/binary"

	bolt "github.com/etcd-io/bbolt"
	"github.com/pkg/errors"
)

// TODO: Interfacification of messages

var foundItem = errors.New("item found")

// aKey singleton for assigning keys to messages
var aKey = new(atomicKey)

// PQueue is a priority queue backed by a Bolt database on disk
type PQueue struct {
	conn *bolt.DB
}

// NewPQueue loads or creates a new PQueue with the given filename
func NewPQueue(filename string) (*PQueue, error) {
	db, err := bolt.Open(filename, 0644, nil)
	if err != nil {
		return nil, err
	}
	return &PQueue{db}, nil
}

func getBucketName(priority int) ([]byte, error) {
	if priority < 0 || priority > 255 {
		return nil, errors.Errorf("invalid priority %d, should in range 0~255", priority)
	}
	return []byte{byte(priority)}, nil
}

func (b *PQueue) enqueueMessage(priority int, key []byte, message *Message) error {
	p, err := getBucketName(priority)
	if err != nil {
		return err
	}
	return b.conn.Update(func(tx *bolt.Tx) error {
		// Get bucket for this priority level
		pb, err := tx.CreateBucketIfNotExists(p)
		if err != nil {
			return err
		}
		err = pb.Put(key, message.value)
		if err != nil {
			return err
		}
		return nil
	})
}

// Enqueue adds a message to the queue
func (b *PQueue) Enqueue(priority int, message *Message) error {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, aKey.Get())
	return b.enqueueMessage(priority, k, message)
}

// Requeue adds a message back into the queue, keeping its precedence.
// If added at the same priority, it should be among the first to dequeue.
// If added at a different priority, it will dequeue before newer messages
// of that priority.
func (b *PQueue) Requeue(priority int, message *Message) error {
	if message.key == nil {
		return errors.New("cannot requeue new message")
	}
	return b.enqueueMessage(priority, message.key, message)
}

// Dequeue removes the oldest, highest priority message from the queue and
// returns it
func (b *PQueue) Dequeue() (*Message, error) {
	var m *Message
	err := b.conn.Update(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(bname []byte, bucket *bolt.Bucket) error {
			if bucket.Stats().KeyN == 0 { //empty bucket
				return nil
			}
			cur := bucket.Cursor()
			k, v := cur.First() //Should not be empty by definition
			priority, _ := binary.Uvarint(bname)
			m = &Message{priority: int(priority), key: cloneBytes(k), value: cloneBytes(v)}

			// Remove message
			if err := cur.Delete(); err != nil {
				return err
			}
			return foundItem //to stop the iteration
		})
		if err != nil && err != foundItem {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Size returns the number of entries of a given priority from 1 to 5
func (b *PQueue) Size(priority int) (int, error) {
	p, err := getBucketName(priority)
	if err != nil {
		return 0, err
	}
	count := 0
	err = b.conn.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(p)
		if bucket == nil {
			return nil
		}
		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

// Close closes the queue and releases all resources
func (b *PQueue) Close() error {
	err := b.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// taken from boltDB. Avoids corruption when re-queueing
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
