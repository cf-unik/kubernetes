package unik

import (
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/api"
	kubetypes "k8s.io/kubernetes/pkg/types"

	"io"
	"k8s.io/kubernetes/pkg/util/flowcontrol"
	"k8s.io/kubernetes/pkg/util/term"
	"strconv"
	"github.com/emc-advanced-dev/pkg/errors"
	"fmt"
	"github.com/emc-advanced-dev/unik/pkg/client"
	"github.com/emc-advanced-dev/unik/pkg/types"
	"strings"
	"encoding/binary"
	"github.com/golang/glog"
	"sync"
)

type Runtime struct {
	version *version
	unikIp  string
	ownedInstances map[string]*types.Instance
	instancesLock sync.RWMutex
}

func New(simpleVer int, unikIp string) *Runtime {
	return &Runtime{
		version: &version{simpleVer: simpleVer},
		unikIp: unikIp,
		ownedInstances: make(map[string]*types.Instance),
	}
}

// Type returns the type of the container runtime.
func (r *Runtime) Type() string {
	return "Unik"
}

// Version returns the version information of the container runtime.
func (r *Runtime) Version() (kubecontainer.Version, error) {
	return r.version, nil
}

// APIVersion returns the cached API version information of the container
// runtime. Implementation is expected to update this cache periodically.
// This may be different from the runtime engine's version.
func (r *Runtime) APIVersion() (kubecontainer.Version, error) {
	return r.Version()
}

type version struct {
	simpleVer int
}

func (v *version) Compare(other string) (int, error) {
	i, err := strconv.Atoi(other)
	if err != nil {
		return 0, errors.New(other + " not an int", err)
	}
	if v.simpleVer < i {
		return -1, nil
	}
	if v.simpleVer > i {
		return 1, nil
	}
	return 0, nil
}
func (v *version) String() string {
	return fmt.Sprintf("%d", v.simpleVer)
}

// Status returns error if the runtime is unhealthy; nil otherwise.
func (r *Runtime) Status() error {
	_, err := client.UnikClient(r.unikIp).AvailableCompilers()
	return err
}

// GetPods returns a list of containers grouped by pods. The boolean parameter
// specifies whether the runtime returns all containers including those already
// exited and dead containers (used for garbage collection).
func (r *Runtime) GetPods(all bool) ([]*kubecontainer.Pod, error) {
	instances, err := client.UnikClient(r.unikIp).Instances().All()
	if err != nil {
		return nil, errors.New("getting instance list from unik daemon", err)
	}
	pods := []*kubecontainer.Pod{}
	for _, instance := range instances {
		if all || instance.State == types.InstanceState_Running {
			pods = append(pods, convertInstance(instance))
		}
	}
	return pods, nil
}

// GarbageCollect removes dead containers using the specified container gc policy
// If allSourcesReady is not true, it means that kubelet doesn't have the
// complete list of pods from all avialble sources (e.g., apiserver, http,
// file). In this case, garbage collector should refrain itself from aggressive
// behavior such as removing all containers of unrecognized pods (yet).
func (r *Runtime) GarbageCollect(gcPolicy kubecontainer.ContainerGCPolicy, allSourcesReady bool) error {
	glog.V(4).Infof("unik: Garbage collecting triggered with policy %v", gcPolicy)

	instances, err := client.UnikClient(r.unikIp).Instances().All()
	if err != nil {
		return errors.New("getting instance list from unik daemon", err)
	}
	instancesToClean := []*types.Instance{}
	for _, instance := range instances {
		switch instance.State {
		case types.InstanceState_Error:
			fallthrough
		case types.InstanceState_Terminated:
			fallthrough
		case types.InstanceState_Stopped:
			instancesToClean = append(instancesToClean, instance)
		}
	}
	for _, instance := range instancesToClean {
		if err := client.UnikClient(r.unikIp).Instances().Delete(instance.Id, false); err != nil {
			return errors.New("cleaning up stopped instance "+instance.Id, err)
		}
	}
	return nil
}

// Syncs the running pod into the desired pod.
func (r *Runtime) SyncPod(pod *api.Pod, apiPodStatus api.PodStatus, podStatus *kubecontainer.PodStatus, pullSecrets []api.Secret, backOff *flowcontrol.Backoff) (result kubecontainer.PodSyncResult) {
	if err := func() error {
		pods, err := r.GetPods(true)
		if err != nil {
			return errors.New("getting all pods", err)
		}
		var currentPod *api.Pod
		for _, knownPod := range pods {
			if knownPod.ID == pod.UID {
				currentPod = knownPod
				break
			}
		}
		if currentPod == nil {
			glog.V(4).Infof("unik: pod to sync: %v no longer found, creating a new one", pod)

		}
		return nil
	}(); err != nil {
		result.Fail(err)
	}
	return
}

// KillPod kills all the containers of a pod. Pod may be nil, running pod must not be.
// gracePeriodOverride if specified allows the caller to override the pod default grace period.
// only hard kill paths are allowed to specify a gracePeriodOverride in the kubelet in order to not corrupt user data.
// it is useful when doing SIGKILL for hard eviction scenarios, or max grace period during soft eviction scenarios.
func (r *Runtime) KillPod(pod *api.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) error {}

// GetPodStatus retrieves the status of the pod, including the
// information of all containers in the pod that are visble in Runtime.
func (r *Runtime) GetPodStatus(uid kubetypes.UID, name, namespace string) (*kubecontainer.PodStatus, error) {}

// PullImage pulls an image from the network to local storage using the supplied
// secrets if necessary.
func (r *Runtime) PullImage(image kubecontainer.ImageSpec, pullSecrets []api.Secret) error {}

// IsImagePresent checks whether the container image is already in the local storage.
func (r *Runtime) IsImagePresent(image kubecontainer.ImageSpec) (bool, error) {}

// Gets all images currently on the machine.
func (r *Runtime) ListImages() ([]kubecontainer.Image, error) {}

// Removes the specified image.
func (r *Runtime) RemoveImage(image kubecontainer.ImageSpec) error {}

// Returns Image statistics.
func (r *Runtime) ImageStats() (*kubecontainer.ImageStats, error) {}

// Returns the filesystem path of the pod's network namespace; if the
// runtime does not handle namespace creation itself, or cannot return
// the network namespace path, it should return an error.
// by all containers in the pod.
func (r *Runtime) GetNetNS(containerID kubecontainer.ContainerID) (string, error) {}

// Returns the container ID that represents the Pod, as passed to network
// plugins. For example, if the runtime uses an infra container, returns
// the infra container's ContainerID.
func (r *Runtime) GetPodContainerID(*kubecontainer.Pod) (kubecontainer.ContainerID, error) {}

// GetContainerLogs returns logs of a specific container. By
// default, it returns a snapshot of the container log. Set 'follow' to true to
// stream the log. Set 'follow' to false and specify the number of lines (e.g.
// "100" or "all") to tail the log.
func (r *Runtime) GetContainerLogs(pod *api.Pod, containerID kubecontainer.ContainerID, logOptions *api.PodLogOptions, stdout, stderr io.Writer) (err error) {}

// Delete a container. If the container is still running, an error is returned.
func (r *Runtime) DeleteContainer(containerID kubecontainer.ContainerID) error {}

func (r *Runtime) AttachContainer(id kubecontainer.ContainerID, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan term.Size) (err error) {}

func (r *Runtime) ExecInContainer(containerID kubecontainer.ContainerID, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan term.Size) error {}

// Forward the specified port from the specified pod to the stream.
func (r *Runtime) PortForward(pod *kubecontainer.Pod, port uint16, stream io.ReadWriteCloser) error {}



func (r *Runtime) launchPod(pod *api.Pod) error {
	if len(pod.Spec.Containers) != 1 {
		return errors.New("unik can only launch a single-container pod", nil)
	}
	container := pod.Spec.Containers[0]
	instanceName := pod.Namespace+"+"+pod.Name
	imageName := container.Image
	mountPointsToVols := make(map[string]string)
	for _, volumeMount := range container.VolumeMounts {
		volId := volumeMount.Name
		mountPoint := volumeMount.MountPath
		mountPointsToVols[mountPoint] = volId
	}
	if len(container.VolumeMounts) > 0 {
		glog.V(4).Infof("unik: warning: in unik runtime, volumeMount maps to volume.Id. mountPath maps to mountPoint")
		glog.V(4).Infof("unik: kube VolumeMounts: %v translated to unik mountPoints: %v", mountPointsToVols)
	}

	env := make(map[string]string)
	for _, envVar := range container.Env {
		env[envVar.Name] = envVar.Value
	}

	memoryMB := int(container.Resources.Requests.Memory().Value() >> 20)

	//instanceName, imageName string, mountPointsToVols, env map[string]string, memoryMb int, noCleanup, debugMode
	instance, err := client.UnikClient(r.unikIp).Instances().Run(instanceName, imageName, mountPointsToVols, env, memoryMB, false, false)
	if err != nil {
		podString := fmt.Sprintf("%+v", pod)
		return errors.New("running instance for pod spec "+podString, err)
	}
	r.instancesLock.Lock()
	defer r.instancesLock.Unlock()
	r.ownedInstances[instance.Id] = instance
	return nil
}

func convertInstance(instance *types.Instance) *kubecontainer.Pod {
	//instance name = namespace+"+"+name
	split := strings.Split(instance.Name, "+")
	namespace := split[0]
	name := split[1]

	hashStr := fmt.Sprintf("%s", instance.String())
	hash := binary.BigEndian.Uint64([]byte{hashStr})

	var state kubecontainer.ContainerState
	switch instance.State {
	case types.InstanceState_Pending:
		state = kubecontainer.ContainerStateCreated
	case types.InstanceState_Running:
		state = kubecontainer.ContainerStateRunning
	case types.InstanceState_Terminated:
		fallthrough
	case types.InstanceState_Stopped:
		state = kubecontainer.ContainerStateExited
	case types.InstanceState_Unknown:
		state = kubecontainer.ContainerStateUnknown
	}

	//right now we are obeying a one vm - per - pod format; so no worries.
	//one pod = one vm = one "container"
	container := &kubecontainer.Container{
		ID: kubecontainer.ContainerID{
			Type: "unik",
			ID: instance.Id,
		},
		Name: name,
		//"tag" is infrastructure for now
		Image: instance.ImageId + ":" + instance.Infrastructure,
		ImageID: instance.ImageId,
		Hash: hash,
		// State is the state of the container.
		State: state,
	}

	return &kubecontainer.Pod{
		ID: kubetypes.UID(instance.Id),
		Name: name,
		Namespace: namespace,
		Containers: []*kubecontainer.Container{container},
	}
}
