package osv

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/emc-advanced-dev/pkg/errors"
	"github.com/emc-advanced-dev/unik/pkg/types"
	unikutil "github.com/emc-advanced-dev/unik/pkg/util"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
)

// javaProjectConfig defines available inputs
type javaProjectConfig struct {
	MainFile    string `yaml:"main_file"`
	RuntimeArgs string `yaml:"runtime_args"`
	BuildCmd    string `yaml:"build_command"`
}

// CreateImageJava creates OSv image from project source code and returns filepath of it.
func CreateImageJava(params types.CompileImageParams, useEc2Bootstrap bool) (string, error) {
	sourcesDir := params.SourcesDir

	var config javaProjectConfig
	data, err := ioutil.ReadFile(filepath.Join(sourcesDir, "manifest.yaml"))
	if err != nil {
		return "", errors.New("failed to read manifest.yaml file", err)
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return "", errors.New("failed to parse yaml manifest.yaml file", err)
	}

	container := unikutil.NewContainer("compilers-osv-java").WithVolume("/dev", "/dev").WithVolume(sourcesDir+"/", "/project_directory")
	var args []string
	if useEc2Bootstrap {
		args = append(args, "-ec2")
	}

	args = append(args, "-main_file", config.MainFile)
	args = append(args, "-args", params.Args)
	if config.BuildCmd != "" {
		args = append(args, "-buildCmd", config.BuildCmd)
	}
	if len(config.RuntimeArgs) > 0 {
		args = append(args, "-runtime", config.RuntimeArgs)
	}

	logrus.WithFields(logrus.Fields{
		"args": args,
	}).Debugf("running compilers-osv-java container")

	if err := container.Run(args...); err != nil {
		return "", errors.New("failed running compilers-osv-java on "+sourcesDir, err)
	}

	resultFile, err := ioutil.TempFile("", "osv-boot.vmdk.")
	if err != nil {
		return "", errors.New("failed to create tmpfile for result", err)
	}
	defer func() {
		if err != nil && !params.NoCleanup {
			os.Remove(resultFile.Name())
		}
	}()

	if err := os.Rename(filepath.Join(sourcesDir, "boot.qcow2"), resultFile.Name()); err != nil {
		return "", errors.New("failed to rename result file", err)
	}
	return resultFile.Name(), nil
}
