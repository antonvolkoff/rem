package rem

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/gedex/inflector"
)

type DB struct {
	sess   *r.Session
	dbName string
}

func NewDB(sess *r.Session, dbName string) *DB {
	return &DB{sess, dbName}
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

	table := d.convertToTableName(t.Elem().Name())
	res, err := r.Db(d.dbName).Table(table).Insert(i).RunWrite(d.sess)
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

	table := d.convertToTableName(t.Elem().Name())
	res, err := r.Db(d.dbName).Table(table).Get(id).Update(i).RunWrite(d.sess)
	if err != nil {
		return err
	}

	if res.Errors != 0 {
		return fmt.Errorf("Document was not updated")
	}

	return nil
}

func (d *DB) Find(i interface{}, term r.Term) error {
	res, err := term.Run(d.sess)
	if err != nil {
		return err
	}
	if res.IsNil() {
		return fmt.Errorf("Not found")
	}

	if reflect.TypeOf(i).Elem().Kind() == reflect.Slice {
		err = res.All(i)
	} else {
		err = res.One(i)
	}
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) Delete(i interface{}) error {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("Passed attribute should be a pointer")
	}

	if d.IsNew(i) {
		return fmt.Errorf("Given document is new and can not be deleted")
	}

	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("Id").String()
	table := d.convertToTableName(t.Elem().Name())

	res, err := r.Db(d.dbName).Table(table).Get(id).Delete().RunWrite(d.sess)
	if err != nil {
		return err
	}

	if res.Errors != 0 {
		return fmt.Errorf("Document was not updated")
	}

	return nil
}

func (d *DB) CreateTable(i interface{}) error {
	var table string

	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		table = t.Name()
	} else {
		table = t.Elem().Name()
	}
	table = d.convertToTableName(table)

	_, err := r.Db(d.dbName).TableCreate(table).RunWrite(d.sess)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) DropTable(i interface{}) error {
	var table string

	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		table = t.Name()
	} else {
		table = t.Elem().Name()
	}
	table = d.convertToTableName(table)

	_, err := r.Db(d.dbName).TableDrop(table).RunWrite(d.sess)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) IndexCreate(i interface{}, name string) error {
	var table string

	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		table = t.Name()
	} else {
		table = t.Elem().Name()
	}
	table = d.convertToTableName(table)

	_, err := r.Db(d.dbName).Table(table).IndexCreate(name).Run(d.sess)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) IndexDrop(i interface{}, name string) error {
	var table string

	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		table = t.Name()
	} else {
		table = t.Elem().Name()
	}
	table = d.convertToTableName(table)

	_, err := r.Db(d.dbName).Table(table).IndexDrop(name).Run(d.sess)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) convertToTableName(name string) string {
	table := strings.ToLower(name)
	table = inflector.Pluralize(table)
	return table
}
