package rem

import (
	"github.com/stretchr/testify/suite"
	"testing"

	r "github.com/dancannon/gorethink"
	"time"
)

type DBSuite struct {
	suite.Suite

	sess *r.Session
	db   *DB
}

func (suite *DBSuite) SetupSuite() {
	suite.sess, _ = r.Connect(r.ConnectOpts{
		Address: "localhost:28015",
		// Database: "rem_test",
	})

	r.DbCreate("rem_test").Run(suite.sess)
	r.Db("rem_test").TableCreate("nodes").Run(suite.sess)

	suite.db = NewDB(suite.sess, "rem_test")
}

func (suite *DBSuite) TearDownSuite() {
	r.DbDrop("rem_test").Run(suite.sess)
}

func (suite *DBSuite) TearDownTest() {
	r.Table("nodes").Delete().RunWrite(suite.sess)
}

func TestDBSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

////////////////////////////////////////////////////////////////////////////////

type Node struct {
	Id        string    `gorethink:"id,omitempty"`
	Name      string    `gorethink:"name"`
	CreatedAt time.Time `gorething:"created_at"`
	UpdatedAt time.Time `gorething:"updated_at"`
}

func (suite *DBSuite) TestInsert() {
	node := Node{Name: "test"}

	err := suite.db.Insert(&node)

	suite.NoError(err)
	suite.NotEmpty(node.Id)
	suite.NotEqual(time.Time{}, node.CreatedAt)
	suite.NotEqual(time.Time{}, node.UpdatedAt)

	var doc Node
	res, _ := r.Db("rem_test").Table("nodes").Get(node.Id).Run(suite.sess)
	res.One(&doc)
	suite.Equal(doc.Name, node.Name)
	suite.Equal(doc.CreatedAt.Second(), node.CreatedAt.Second())
	suite.Equal(doc.UpdatedAt.Second(), node.UpdatedAt.Second())
}

func (suite *DBSuite) TestInsert_NonPointer() {
	node := Node{Name: "test"}
	err := suite.db.Insert(node)

	suite.Error(err)
	suite.Empty(node.Id)
	suite.Equal(time.Time{}, node.CreatedAt)
	suite.Equal(time.Time{}, node.UpdatedAt)
}

func (suite *DBSuite) TestInsert_Existing() {
	node := &Node{Id: "ID", Name: "node"}
	err := suite.db.Insert(node)
	suite.Error(err)
}

func (suite *DBSuite) TestIsNew() {
	node1 := &Node{Name: "test"}
	node2 := &Node{Id: "ID", Name: "test"}

	suite.True(suite.db.IsNew(node1))
	suite.False(suite.db.IsNew(node2))
}

func (suite *DBSuite) TestUpdate() {
	node := &Node{Name: "test"}
	suite.db.Insert(node)

	node.Name = "root"
	updatedAt := node.UpdatedAt
	err := suite.db.Update(node)

	suite.NoError(err)
	suite.NotEqual(updatedAt, node.UpdatedAt)

	var doc Node
	res, _ := r.Db("rem_test").Table("nodes").Get(node.Id).Run(suite.sess)
	res.One(&doc)
	suite.Equal(doc.Name, node.Name)
	suite.Equal(doc.UpdatedAt.Second(), node.UpdatedAt.Second())
}

func (suite *DBSuite) TestFind() {
	node := &Node{Name: "test"}
	suite.db.Insert(node)

	var n Node
	err := suite.db.Find(&n, r.Db("rem_test").Table("nodes").Get(node.Id))

	suite.NoError(err)
	suite.Equal(node.Id, n.Id)
}

func (suite *DBSuite) TestFind_Array() {
	node1 := &Node{Name: "a"}
	node2 := &Node{Name: "b"}
	suite.db.Insert(node1)
	suite.db.Insert(node2)

	var ns []Node
	err := suite.db.Find(&ns, r.Db("rem_test").Table("nodes").OrderBy("name"))

	suite.NoError(err)
	var ids []string
	for _, n := range ns {
		ids = append(ids, n.Id)
	}
	suite.Contains(ids, node1.Id)
	suite.Contains(ids, node2.Id)
}

func (suite *DBSuite) TestDelete() {
	node := &Node{Name: "root"}
	suite.db.Insert(node)

	err := suite.db.Delete(node)

	suite.NoError(err)
	res, _ := r.Db("rem_test").Table("nodes").Get(node.Id).Run(suite.sess)
	suite.True(res.IsNil())
}

func (suite *DBSuite) TestDelete_NonPtr() {
	node := Node{Name: "root"}
	suite.db.Insert(&node)

	err := suite.db.Delete(node)

	suite.Error(err)
}

func (suite *DBSuite) TestDelete_New() {
	node := &Node{Name: "root"}

	err := suite.db.Delete(node)

	suite.Error(err)
}

func (suite *DBSuite) TestCreateTable() {
	type User struct{}

	err := suite.db.CreateTable(User{})

	suite.NoError(err)
	var tables []string
	res, _ := r.Db("rem_test").TableList().Run(suite.sess)
	res.All(&tables)
	suite.Contains(tables, "users")

	r.Db("rem_test").TableDrop("users").RunWrite(suite.sess)
}

func (suite *DBSuite) TestDropTabale() {
	type User struct{}
	suite.db.CreateTable(User{})

	err := suite.db.DropTable(User{})

	suite.NoError(err)
	var tables []string
	res, _ := r.Db("rem_test").TableList().Run(suite.sess)
	res.All(&tables)
	suite.NotContains(tables, "users")
}
