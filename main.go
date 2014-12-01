package main

import(
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
  "gopkg.in/yaml.v2"
	"github.com/armon/consul-api"
  "os"
  "io/ioutil"
)

//Connect establishes a connection to local running consul agent.
//Currently only localhost:8500 is supported.
func Connect() *consulapi.Client {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}

	return client
}

type BackupKV struct {
  Key, Value string
}

func main() {
	app := cli.NewApp()
	app.Name = "consul-backup"
	app.Usage = "backup consul kv database!"
	app.Version = "0.0.1"
	app.Commands = []cli.Command{
		{
			Name:      "version",
			ShortName: "v",
			Usage:     "consul-backup version",
			Action: func(c *cli.Context) {
				fmt.Println(app.Version)
			},
		},
		{
			Name:      "backup",
			ShortName: "b",
			Usage:     "backup kv database",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "file",
					Value: "backup.yaml",
					Usage: "backup file",
				},
				cli.StringFlag{
					Name:  "root",
					Value: "",
					Usage: "Root key. Leave blank for all keys",
				},
      },
			Action: func(c *cli.Context) {
				client := Connect()
        kvp, _, _ := client.KV().List(c.String("root"),nil)
        var kv []BackupKV
        for _, a := range kvp {
          kv = append(kv, BackupKV{Key: a.Key, Value: string(a.Value)})
        }

        d, err := yaml.Marshal(kv)
        if err != nil {
           log.Fatalf("error: %v", err)
        }
        fmt.Printf("--- t dump:\n%s\n\n", string(d))
        ioutil.WriteFile(c.String("file"), d, 0644)
			},
		},
		{
			Name:      "restore",
			ShortName: "r",
			Usage:     "restore kv database",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "file",
					Value: "backup.yaml",
					Usage: "backup file",
				},
      },
			Action: func(c *cli.Context) {
				client := Connect()
        var kv []BackupKV
        vals,err:=ioutil.ReadFile(c.String("file"))
        if err != nil {
          log.Fatalf("error: %v", err)
        }
        err = yaml.Unmarshal(vals, &kv)
        if err != nil {
           log.Fatalf("error: %v", err)
        }
        kvc := client.KV()
        for _, a := range kv {
          fmt.Printf("%s\n%s\n", a.Key, string(a.Value))
          _, err := kvc.Put(&consulapi.KVPair{Key: a.Key, Value: []byte(a.Value) }, nil) 
          if err != nil {
            log.Fatalf("error: %v", err)
          }
        }
			},
		},
	}
	app.Run(os.Args)
}
