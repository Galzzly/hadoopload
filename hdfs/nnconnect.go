package nnconnect

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
)

func ConnectToNamenode() (*hdfs.Client, error) {
	return getClient()
}

func getClient() (*hdfs.Client, error) {
	// Check for the namenode in env
	namenode := os.Getenv("HADOOP_NAMENODE")
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, err
	}

	// If namenode is populated, set it in options.Addresses
	options := hdfs.ClientOptionsFromConf(conf)
	if namenode != "" {
		options.Addresses = []string{namenode}
	}

	// Otherwise, just return no client and error
	if options.Addresses == nil {
		return nil, errors.New("cannot find Namenode to connect to")
	}

	// The following code will fail for clusters that have kerberos enabled
	// for the time being. However, for clusters that do not have it enabled
	// it will work just fine. I will, though, be working towards enabling
	// support for kerberos in the future.

	if options.KerberosClient != nil {
		options.KerberosClient, err = getKerberosClient()
		if err != nil {
			return nil, fmt.Errorf("problem with kerberos auth: %s", err)
		}
	} else {
		options.User = os.Getenv("HADOOP_USER_NAME")
		if options.User == "" {
			u, err := user.Current()
			if err != nil {
				return nil, fmt.Errorf("unable to determine user: %s", err)
			}

			options.User = u.Username
		}
	}

	dialFunc := (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 5 * time.Second,
		DualStack: true,
	}).DialContext

	options.NamenodeDialFunc = dialFunc
	options.DatanodeDialFunc = dialFunc

	// With Kerberos enabled, the hdfs.NewClient return failes due to being unable
	// to connect to a namenode. This is likely down to the ciphers used, and so
	// will need to have further testing performed.
	client, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to namenode: %s", err)
	}

	return client, err
}

func getKerberosClient() (*krb.Client, error) {
	conf := os.Getenv("KRB_CONFIG")
	if conf == "" {
		conf = "/etc/krb5.conf"
	}

	cfg, err := config.Load(conf)
	if err != nil {
		return nil, err
	}

	ccachePath := os.Getenv("KRBCCNAME")
	if strings.Contains(ccachePath, ":") {
		if strings.HasPrefix(ccachePath, "FILE:") {
			ccachePath = strings.SplitN(ccachePath, ":", 2)[1]
		} else {
			return nil, fmt.Errorf("unusable ccache: %s", ccachePath)
		}
	} else if ccachePath == "" {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}

		ccachePath = fmt.Sprintf("tmp/krb5cc_%s", u.Uid)
	}

	ccache, err := credentials.LoadCCache(ccachePath)
	if err != nil {
		return nil, err
	}

	client, err := krb.NewFromCCache(ccache, cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}
