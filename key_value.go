package cloudypg

import (
	"context"
	"errors"
	"fmt"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/keyvalue"
	"github.com/go-openapi/strfmt"
	"github.com/jackc/pgx/v5"
)

var _ keyvalue.WritableKeyValueStore = (*KeyValueStore)(nil)

type KeyValueStore struct {
	conn          *pgx.Conn
	table         string
	encryptionKey string
}

func NewKeyValueStore(ctx context.Context, tablename string, conn *pgx.Conn) (*KeyValueStore, error) {
	kv := &KeyValueStore{
		conn:  conn,
		table: tablename,
	}
	err := kv.Init(ctx)
	return kv, err
}

func NewSecretKeyValueStore(ctx context.Context, tablename string, conn *pgx.Conn, encryptionKey string) (*KeyValueStore, error) {
	kv := &KeyValueStore{
		conn:          conn,
		table:         tablename,
		encryptionKey: encryptionKey,
	}
	err := kv.Init(ctx)
	return kv, err
}

func (kv *KeyValueStore) Init(ctx context.Context) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v ( key varchar(1000) primary key, value varchar(4000));`, kv.table)

	_, err := kv.conn.Exec(ctx, sql)
	return err
}

// --- KeyValueStore
func (kv *KeyValueStore) Get(key string) (string, error) {
	ctx := context.Background()
	nkey := keyvalue.NormalizeKey(key)

	sql := fmt.Sprintf("SELECT value from %v WHERE key = $1", kv.table)

	var value string
	err := kv.conn.QueryRow(ctx, sql, nkey).Scan(&value)
	if err == nil {
		return value, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}

	return "", err
}

func (kv *KeyValueStore) GetAll() (map[string]string, error) {
	ctx := context.Background()
	sql := fmt.Sprintf("SELECT key, value from %v", kv.table)

	rows, err := kv.conn.Query(ctx, sql)
	if err != nil {
		fmt.Println(err)
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	rowData, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]string, error) {
		var key string
		var value string
		err := row.Scan(&key, &value)
		if err != nil {
			return nil, err
		}
		return []string{key, value}, nil
	})
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	m := make(map[string]string)
	for _, r := range rowData {
		m[r[0]] = r[1]
	}

	return m, nil
}

// --- FilteredKeyValueStore
func (kv *KeyValueStore) GetWithPrefix(prefix string) (map[string]string, error) {
	return nil, nil
}

// --- WritableKeyValueStore
func (kv *KeyValueStore) Set(key string, value string) error {
	ctx := context.Background()
	nkey := keyvalue.NormalizeKey(key)
	sql := fmt.Sprintf(`INSERT INTO %v (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value=$2;`, kv.table)

	_, err := kv.conn.Exec(ctx, sql, nkey, value)

	return err
}
func (kv *KeyValueStore) SetMany(items map[string]string) error {
	ctx := context.Background()

	batch := &pgx.Batch{}
	for key, value := range items {
		nkey := keyvalue.NormalizeKey(key)
		sqlUpsert := fmt.Sprintf(`INSERT INTO %v (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value=$2;`, kv.table)
		batch.Queue(sqlUpsert, nkey, value)
	}

	br := kv.conn.SendBatch(ctx, batch)
	_, err := br.Exec()
	if err != nil {
		return err
	}

	err = br.Close()
	if err != nil {
		return err
	}

	return err
}
func (kv *KeyValueStore) Delete(key string) error {
	ctx := context.Background()
	nkey := keyvalue.NormalizeKey(key)
	sql := fmt.Sprintf("DELETE FROM %v WHERE key = $1", kv.table)
	_, err := kv.conn.Exec(ctx, sql, nkey)

	return err
}

func (kv *KeyValueStore) GetSecure(key string) (strfmt.Password, error) {
	if kv.encryptionKey == "" {
		return "", errors.New("no encryption key available")
	}

	encryptedValue, err := kv.Get(key)
	if err != nil {
		return "", err
	}

	value, err := cloudy.AesDecrypt(encryptedValue, kv.encryptionKey)
	if err != nil {
		return "", err
	}

	return strfmt.Password(value), nil
}

func (kv *KeyValueStore) SetSecure(key string, value strfmt.Password) error {
	if kv.encryptionKey == "" {
		return errors.New("no encryption key available")
	}

	encryptedValue, err := cloudy.AesEncrypt(string(value), key)
	if err != nil {
		return err
	}

	return kv.Set(key, encryptedValue)
}
