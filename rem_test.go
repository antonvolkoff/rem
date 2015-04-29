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

	var doc Node
	res, _ := r.Table("Node").Get(node.Id).Run(sess)
	res.One(&doc)
	assert.Equal(t, doc.Name, node.Name)
	assert.Equal(t, doc.CreatedAt.Second(), node.CreatedAt.Second())
	assert.Equal(t, doc.UpdatedAt.Second(), node.UpdatedAt.Second())
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

func TestDB_Insert_Existing(t *testing.T) {
	db := NewDB(nil)
	node := &Node{Id: "ID", Name: "node"}

	err := db.Insert(node)

	assert.Error(t, err)
}

func TestDB_IsNew(t *testing.T) {
	db := NewDB(nil)
	node1 := &Node{Name: "test"}
	node2 := &Node{Id: "ID", Name: "test"}

	assert.True(t, db.IsNew(node1))
	assert.False(t, db.IsNew(node2))
}

func TestDB_Update(t *testing.T) {
	sess := session()
	defer deleteAll(sess)
	db := NewDB(sess)

	node := &Node{Name: "test"}
	db.Insert(node)

	node.Name = "root"
	updatedAt := node.UpdatedAt
	err := db.Update(node)

	assert.NoError(t, err)
	assert.NotEqual(t, updatedAt, node.UpdatedAt)

	var doc Node
	res, _ := r.Table("Node").Get(node.Id).Run(sess)
	res.One(&doc)
	assert.Equal(t, doc.Name, node.Name)
	assert.Equal(t, doc.UpdatedAt.Second(), node.UpdatedAt.Second())
}

func TestDB_Find(t *testing.T) {
	sess := session()
	defer deleteAll(sess)
	db := NewDB(sess)

	node := &Node{Name: "test"}
	db.Insert(node)

	var n Node
	err := db.Find(&n, r.Table("Node").Get(node.Id))

	assert.NoError(t, err)
	assert.Equal(t, node.Id, n.Id)
}

func TestDB_Find_Array(t *testing.T) {
	sess := session()
	defer deleteAll(sess)
	db := NewDB(sess)

	node1 := &Node{Name: "a"}
	node2 := &Node{Name: "b"}
	db.Insert(node1)
	db.Insert(node2)

	var ns []Node
	err := db.Find(&ns, r.Table("Node").OrderBy("name"))

	assert.NoError(t, err)
	assert.Equal(t, node1.Id, ns[0].Id)
	assert.Equal(t, node2.Id, ns[1].Id)
}
