package mysqlutils

import (
	"errors"
	"fmt"
	"strings"

	"github.com/andsha/securestorage"
	"github.com/andsha/vconfig"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	//_ "github.com/ziutek/mymysql/thrsafe"
)

type (
	MysqlProcess struct {
		mysqlDB mysql.Conn
	}
)

func NewDB(host, port, user, password, passwordsrt string, pwdSection *vconfig.Section) (*MysqlProcess, error) {
	raddr := fmt.Sprintf("%v:%v", host, port)
	var keyStorage *securestorage.SecureStorage
	if pwdSection != nil {
		var err error
		keyStorage, err = securestorage.NewSecureStorage("", "", pwdSection)
		if err != nil {
			return nil, err
		}
	}
	if passwordsrt == "" {
		if password != "" {
			var err error
			if strings.HasSuffix(password, ".key") {
				password, err = keyStorage.GetPasswordFromFile(password)
			} else {
				password, err = keyStorage.GetPasswordFromString(password)
			}
			if err != nil {
				return nil, err
			}
		}
	} else {
		password = passwordsrt
	}

	db := mysql.New("tcp", "", raddr, user, password)
	if err := db.Connect(); err != nil {
		return nil, err
	}
	if !db.IsConnected() {
		return nil, errors.New("Cannot connect to MySQL")
	}
	mysqlProcess := new(MysqlProcess)
	mysqlProcess.mysqlDB = db

	return mysqlProcess, nil
}

func (process *MysqlProcess) Close() error {
	return nil
}

func (process *MysqlProcess) Run(sql string) ([][]interface{}, error) {
	rows, res, err := process.mysqlDB.Query(sql)
	if err != nil {
		return nil, err
	}
	ncols := len(res.Fields())
	result := make([][]interface{}, 0)
	for _, r := range rows {
		re := make([]interface{}, 0)
		for i := 0; i < ncols; i++ {
			re = append(re, r.Str(i))
		}
		result = append(result, re)
	}
	return result, nil
}
