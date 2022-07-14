package cmd

import (
	"mindb"
	"strconv"
)

func hSet(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 3 {

		err = SyntaxErr

		return

	}

	var count int

	if count, err = db.HSet([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {

		res = strconv.Itoa(count)

	}

	return

}

func hSetNx(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 3 {

		err = SyntaxErr

		return

	}

	var ok bool

	if ok, err = db.HSetNx([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {

		if ok {

			res = "1"

		} else {

			res = "0"

		}

	}

	return

}

func hGet(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 2 {

		err = SyntaxErr

		return

	}

	val := db.HGet([]byte(args[0]), []byte(args[1]))

	if len(val) == 0 {

		res = "<nil>"

	} else {

		res = string(val)

	}

	return

}

func hGetAll(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 1 {

		err = SyntaxErr

		return

	}

	val := db.HGetAll([]byte(args[0]))

	for i, v := range val {

		res += string(v)

		if i != len(val)-1 {

			res += "\n"

		}

	}

	return

}

func hDel(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) <= 1 {

		err = SyntaxErr

		return

	}

	var fields [][]byte

	for _, f := range args[1:] {

		fields = append(fields, []byte(f))

	}

	var count int

	if count, err = db.HDel([]byte(args[0]), fields...); err == nil {

		res = strconv.Itoa(count)

	}

	return

}

func hExists(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 2 {

		err = SyntaxErr

		return

	}

	if exists := db.HExists([]byte(args[0]), []byte(args[1])); exists {

		res = "1"

	} else {

		res = "0"

	}

	return

}

func hLen(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 1 {

		err = SyntaxErr

		return

	}

	count := db.HLen([]byte(args[0]))

	res = strconv.Itoa(count)

	return

}

func hKeys(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 1 {

		err = SyntaxErr

		return

	}

	val := db.HKeys([]byte(args[0]))

	for i, v := range val {

		res += v

		if i != len(val)-1 {

			res += "\n"

		}

	}

	return

}

func hValues(db *mindb.MinDB, args []string) (res string, err error) {

	if len(args) != 1 {

		err = SyntaxErr

		return

	}

	val := db.HValues([]byte(args[0]))

	for i, v := range val {

		res += string(v)

		if i != len(val)-1 {

			res += "\n"

		}

	}

	return

}

func init() {

	addExecCommand("hset", hSet)

	addExecCommand("hsetnx", hSetNx)

	addExecCommand("hget", hGet)

	addExecCommand("hgetall", hGetAll)

	addExecCommand("hdel", hDel)

	addExecCommand("hexists", hExists)

	addExecCommand("hlen", hLen)

	addExecCommand("hkeys", hKeys)

	addExecCommand("hvalues", hValues)

}
