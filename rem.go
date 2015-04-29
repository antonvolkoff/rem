package rem

import (
	r "github.com/dancannon/gorethink"
	"reflect"
	"time"

	"fmt"
)

type DB struct {
	sess *r.Session
}

func NewDB(sess *r.Session) *DB {
	return &DB{sess}
}

func (d *DB) Insert(i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("Passed attribute should be a pointer")
	}

	tableName := t.Elem().Name()

	res, err := r.Table(tableName).Insert(i).RunWrite(d.sess)
	if err != nil {
		return err
	}

	s := reflect.ValueOf(i).Elem()
	s.FieldByName("Id").SetString(res.GeneratedKeys[0])
	s.FieldByName("CreatedAt").Set(reflect.ValueOf(time.Now()))
	s.FieldByName("UpdatedAt").Set(reflect.ValueOf(time.Now()))

	return nil
}
