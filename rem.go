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

	if !d.IsNew(i) {
		return fmt.Errorf("Given document is not new")
	}

	s := reflect.ValueOf(i).Elem()
	s.FieldByName("CreatedAt").Set(reflect.ValueOf(time.Now()))
	s.FieldByName("UpdatedAt").Set(reflect.ValueOf(time.Now()))

	table := t.Elem().Name()
	res, err := r.Table(table).Insert(i).RunWrite(d.sess)
	if err != nil {
		return err
	}

	s.FieldByName("Id").SetString(res.GeneratedKeys[0])

	return nil
}

func (d *DB) IsNew(i interface{}) bool {
	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("Id").String()

	if id != "" {
		return false
	}

	return true
}

func (d *DB) Update(i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("Passed attribute should be a pointer")
	}

	if d.IsNew(i) {
		return fmt.Errorf("Given document is new and can not be updated")
	}

	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("Id").String()
	s.FieldByName("UpdatedAt").Set(reflect.ValueOf(time.Now()))

	table := t.Elem().Name()
	res, err := r.Table(table).Get(id).Update(i).RunWrite(d.sess)
	if err != nil {
		return err
	}

	if res.Errors != 0 {
		return fmt.Errorf("Document was not updated")
	}

	return nil
}
