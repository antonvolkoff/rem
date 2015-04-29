package rem

import (
	"github.com/stretchr/testify/assert"
	"testing"

	r "github.com/dancannon/gorethink"
	"time"
)

func session() *r.Session {
	sess, _ := r.Connect(r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "rem_test",
	})

	r.DbCreate("rem_test").Run(sess)
	r.Db("rem_test").TableCreate("Node").Run(sess)

	return sess
}

func deleteAll(sess *r.Session) {
	r.DbDrop("rem_test").Run(sess)
}

type Node struct {
	Id        string    `gorethink:"id,omitempty"`
	Name      string    `gorethink:"name"`
	CreatedAt time.Time `gorething:"created_at"`
	UpdatedAt time.Time `gorething:"updated_at"`
}

func TestDB_Insert(t *testing.T) {
	sess := session()
	defer deleteAll(sess)

	node := Node{Name: "test"}
	db := NewDB(sess)

	err := db.Insert(&node)

	assert.NoError(t, err)
	assert.NotEmpty(t, node.Id)
	assert.NotEqual(t, time.Time{}, node.CreatedAt)
	assert.NotEqual(t, time.Time{}, node.UpdatedAt)
}

func TestDB_Insert_NonPointer(t *testing.T) {
	sess := session()
	defer deleteAll(sess)

	node := Node{Name: "test"}
	db := NewDB(sess)

	err := db.Insert(node)

	assert.Error(t, err)
	assert.Empty(t, node.Id)
	assert.Equal(t, time.Time{}, node.CreatedAt)
	assert.Equal(t, time.Time{}, node.UpdatedAt)
}

func TestDB_IsNew(t *testing.T) {
	db := NewDB(nil)
	node1 := &Node{Name: "test"}
	node2 := &Node{Id: "ID", Name: "test"}

	assert.True(t, db.IsNew(node1))
	assert.False(t, db.IsNew(node2))
}
