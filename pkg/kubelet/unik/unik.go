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
	"os"
	"github.com/emc-advanced-dev/unik/pkg/config"
)

type Runtime struct {
	version          *version
	unikIp           string
	ownedInstances   map[string]*types.Instance
	podsToInstances  map[string]*types.Instance
	instanceRestarts map[string]int
	hubConfig        string
	mapLock          sync.RWMutex
}

func New(simpleVer int, unikIp string) *Runtime {
	hubUrl := os.Getenv("UNIK_HUB_URL")
	if hubUrl == "" {
		hubUrl = "http://hub.project-unik.io"
		glog.V(4).Infof("unik: no UNIK_HUB_URL provided, using default %v", hubUrl)
	}
	hubUser := os.Getenv("UNIK_HUB_USER")
	hubPass := os.Getenv("UNIK_HUB_PASSWORD")
	return &Runtime{
		version: &version{simpleVer: simpleVer},
		unikIp: unikIp,
		ownedInstances: make(map[string]*types.Instance),
		instanceRestarts: make(map[string]int),
		podsToInstances: make(map[string]int),
		hubConfig: config.HubConfig{
			URL: hubUrl,
			Username: hubUser,
			Password: hubPass,
		},
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
			return errors.New("cleaning up stopped instance " + instance.Id, err)
		}
	}
	return nil
}

// Syncs the running pod into the desired pod.
func (r *Runtime) SyncPod(desiredPod *api.Pod, desiredPodStatus api.PodStatus, internalPodStatus *kubecontainer.PodStatus, pullSecrets []api.Secret, backOff *flowcontrol.Backoff) (result kubecontainer.PodSyncResult) {
	if err := func() error {
		if len(desiredPod.Spec.Containers) != 1 {
			podString := fmt.Sprintf("%+v", desiredPod.Spec)
			return errors.New("unik can only manage single-container pods; you gave me " + podString, nil)
		}
		if len(desiredPodStatus.ContainerStatuses) != 1 {
			statusString := fmt.Sprintf("%+v", desiredPodStatus)
			return errors.New("unik can only manage single-container pods; you gave me this status " + statusString, nil)
		}
		desiredContainer := desiredPod.Spec.Containers[0]

		internalPod := kubecontainer.ConvertPodStatusToRunningPod(r.Type(), internalPodStatus)
		internalContainer := internalPod.FindContainerByName(desiredContainer.Name)
		if internalContainer == nil {
			glog.V(4).Infof("unik: container to sync: %v no longer found, creating a new one", desiredContainer)
			if _, err := r.runPod(desiredPod); err != nil {
				return errors.New("launching pod", err)
			}
			glog.V(4).Infof("unik: instance launched successfully", desiredContainer)
			result.AddSyncResult(&kubecontainer.SyncResult{
				Action: kubecontainer.StartContainer,
				Target: desiredContainer.Name,
				Message: "instance started",
			})
			return nil
		}

		desiredHash := hash(hashable{
			state: desiredPodStatus.ContainerStatuses[0].State,
			namespace: desiredPod.Namespace,
			name: desiredPod.Name,
			image: desiredContainer.Image,
		})

		syncNeeded := internalContainer.Hash != desiredHash
		if syncNeeded {
			glog.V(4).Infof("sync needed: desired pod: %+v; desired hash: %v\ninternal pod: %+v, internal hash: %v", desiredPod, desiredHash, internalPod, internalContainer.Hash)
			if err := r.KillPod(nil, internalPod, nil); err != nil {
				return errors.New("deleting out-of-sync pod " + internalPod.ID, err)
			}
			result.AddSyncResult(kubecontainer.SyncResult{
				Action: kubecontainer.KillContainer,
				Target: desiredContainer.Name,
				Message: "out of sync instance killed",
			})
			instance, err := r.runPod(desiredPod)
			if err != nil {
				return errors.New("launching pod", err)
			}
			glog.V(4).Infof("unik: instance launched successfully", desiredContainer)
			result.AddSyncResult(&kubecontainer.SyncResult{
				Action: kubecontainer.StartContainer,
				Target: desiredContainer.Name,
				Message: "instance started",
			})
			r.mapLock.Lock()
			defer r.mapLock.Unlock()
			r.instanceRestarts[instance.Id] += 1

			return nil
		} else {
			glog.V(4).Infof("no sync needed: for pod %+v", desiredPod)
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
func (r *Runtime) KillPod(_ *api.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) error {
	instanceName := getInstanceName(runningPod.Namespace, runningPod.Name)
	instance, err := client.UnikClient(r.unikIp).Instances().Get(instanceName)
	if err != nil {
		return errors.New("could not find instance " + instanceName, err)
	}
	if err := client.UnikClient(r.unikIp).Instances().Delete(instance.Id, true); err == nil {
		return errors.New("deleting instance " + instance.Id, err)
	}
	r.mapLock.Lock()
	defer r.mapLock.Unlock()
	delete(r.ownedInstances, instance.Id)
	delete(r.podsToInstances, runningPod.ID)
	return nil
}

// GetPodStatus retrieves the status of the pod, including the
// information of all containers in the pod that are visble in Runtime.
func (r *Runtime) GetPodStatus(uid kubetypes.UID, name, namespace string) (*kubecontainer.PodStatus, error) {
	instance, err := client.UnikClient(r.unikIp).Instances().Get(getInstanceName(namespace, name))
	if err != nil {
		return nil, errors.New("getting instance from unik", err)
	}
	state := toContainerState(instance.State)
	imageName := getImageName(instance.ImageId, instance.Infrastructure)
	newHash := hash(hashable{
		state: state,
		namespace: namespace,
		name: name,
		image: imageName,
	})
	r.mapLock.RLock()
	defer r.mapLock.RUnlock()
	restarts := r.instanceRestarts[instance.Id]

	return &kubecontainer.PodStatus{
		ID: uid,
		Name: name,
		Namespace: namespace,
		IP: instance.IpAddress,
		ContainerStatuses: []*kubecontainer.ContainerStatus{
			&kubecontainer.ContainerStatus{
				ID: instance.Id,
				Name: instance.Name,
				State: state,
				CreatedAt: instance.Created,
				StartedAt: instance.Created,
				Image: imageName,
				ImageID: instance.ImageId,
				Hash: newHash,
				RestartCount: restarts,
			},
		},
	}
}

// PullImage pulls an image from the network to local storage using the supplied
// secrets if necessary.
func (r *Runtime) PullImage(image kubecontainer.ImageSpec, pullSecrets []api.Secret) error {
	imageName, infrastructure := getImageInfo(image.Image)
	//TODO: this may not be the format in the future, but currently works...
	provider := strings.ToLower(infrastructure)
	return client.UnikClient(r.unikIp).Images().Pull(r.hubConfig, imageName, provider)
}

// IsImagePresent checks whether the container image is already in the local storage.
func (r *Runtime) IsImagePresent(image kubecontainer.ImageSpec) (bool, error) {
	imageName, _ := getImageInfo(image.Image)
	_, err := client.UnikClient(r.unikIp).Images().Get(imageName)
	return err == nil, nil
}

// Gets all images currently on the machine.
func (r *Runtime) ListImages() ([]kubecontainer.Image, error) {
	images, err := client.UnikClient(r.unikIp).Images().All()
	if err != nil {
		return nil, errors.New("getting image list", err)
	}
	kubeImages := []kubecontainer.Image{}
	for _, image := range images {
		kubeImages = append(kubeImages, convertImage(image))
	}
	return kubeImages, nil
}

// Removes the specified image.
func (r *Runtime) RemoveImage(image kubecontainer.ImageSpec) error {
	imageName, _ := getImageInfo(image.Image)
	return client.UnikClient(r.unikIp).Images().Delete(imageName, true)
}

// Returns Image statistics.
func (r *Runtime) ImageStats() (*kubecontainer.ImageStats, error) {
	return &kubecontainer.ImageStats{
		TotalStorageBytes: 1,
	}, nil
}

// Returns the filesystem path of the pod's network namespace; if the
// runtime does not handle namespace creation itself, or cannot return
// the network namespace path, it should return an error.
// by all containers in the pod.
func (r *Runtime) GetNetNS(containerID kubecontainer.ContainerID) (string, error) {
	return "", errors.New("unik runtime does not handle namespace creation", nil)
}

// Returns the container ID that represents the Pod, as passed to network
// plugins. For example, if the runtime uses an infra container, returns
// the infra container's ContainerID.
func (r *Runtime) GetPodContainerID(pod *kubecontainer.Pod) (kubecontainer.ContainerID, error) {
	instance, ok := r.podsToInstances[pod.ID]
	if !ok {
		return kubecontainer.ContainerID{}, errors.New("instance not found for pod "+string(pod.ID), nil)
	}
	return kubecontainer.ContainerID{
		Type: r.Type(),
		ID: instance.Id,
	}, nil
}

// GetContainerLogs returns logs of a specific container. By
// default, it returns a snapshot of the container log. Set 'follow' to true to
// stream the log. Set 'follow' to false and specify the number of lines (e.g.
// "100" or "all") to tail the log.
func (r *Runtime) GetContainerLogs(pod *api.Pod, _ kubecontainer.ContainerID, logOptions *api.PodLogOptions, stdout, _ io.Writer) (err error) {
	instance, ok := r.podsToInstances[pod.UID]
	if !ok {
		return errors.New("instance not found for pod "+string(pod.UID), nil)
	}
	follow := false
	tailLines := 0
	if logOptions != nil {
		follow = logOptions.Follow
		if logOptions.TailLines != nil {
			tailLines = *logOptions.TailLines
		}
	}
	if follow {
		stream, err := client.UnikClient(r.unikIp).Instances().AttachLogs(instance.Id, false)
		if err != nil {
			return errors.New("failed to attach to logs", err)
		}
		if _, err := io.Copy(stdout, stream); err != nil {
			return errors.New("copying from stream to stdout", err)
		}
	} else {
		logs, err := client.UnikClient(r.unikIp).Instances().GetLogs(instance.Id)
		if err != nil {
			return errors.New("failed to get logs", err)
		}
		if tailLines > 0 {
			logLines := strings.Split(logs, "\n")
			if len(logLines) < tailLines {
				tailLines = len(logLines)
			}
			logs = strings.Join(logLines[tailLines-1:], "\n")
		}
		if _, err := stdout.Write([]byte(logs)); err != nil {
			return errors.New("writing logs to stdout", err)
		}
	}
	return nil
}

// Delete a container. If the container is still running, an error is returned.
func (r *Runtime) DeleteContainer(containerID kubecontainer.ContainerID) error {
	return errors.New("don't kill the container, kill the pod", nil)
}

func (r *Runtime) AttachContainer(id kubecontainer.ContainerID, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan term.Size) (err error) {
	return errors.New("attaching to unikernels not currently supported in unik", nil)
}

func (r *Runtime) ExecInContainer(containerID kubecontainer.ContainerID, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan term.Size) error {
	return errors.New("cannot execute commands in unikernels", nil)
}

// Forward the specified port from the specified pod to the stream.
func (r *Runtime) PortForward(pod *kubecontainer.Pod, port uint16, stream io.ReadWriteCloser) error {
	return errors.New("port forwarding not yet implemented", nil)
}

func (r *Runtime) runPod(pod *api.Pod) (*types.Instance, error) {
	container := pod.Spec.Containers[0]
	instanceName := getInstanceName(pod.Namespace, pod.Name)
	imageName, _ := getImageInfo(container.Image)
	//because we store the image name as Name:Infrastructure
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
		return nil, errors.New("running instance for pod spec " + podString, err)
	}
	r.mapLock.Lock()
	defer r.mapLock.Unlock()
	r.ownedInstances[instance.Id] = instance
	r.podsToInstances[pod.UID] = instance
	return instance, nil
}


//information about an instance/pod to hash
type hashable struct {
	state     kubecontainer.ContainerState
	namespace string
	name      string
	image     string
}

func hash(info hashable) uint64 {
	hashStr := fmt.Sprintf("{state: %v, namespace: %v, name: %v, image: %v}", info.state, info.namespace, info.name, info.image)
	return binary.BigEndian.Uint64([]byte{hashStr})
}

func getInstanceName(namespace, name string) string {
	return namespace + "+" + name
}

func getImageName(imageId, infrastructure string) string {
	return imageId + ":" + infrastructure
}

func getImageInfo(kubernetesImageName string) (string, string) {
	split := strings.Split(kubernetesImageName, ":")
	if len(split) != 2 {
		panic("image format should be NAME:INFRASTRUCTURE, but have "+kubernetesImageName)
	}
	return split[0], split[1]
}

func toContainerState(instanceState types.InstanceState) kubecontainer.ContainerState {
	var state kubecontainer.ContainerState
	switch instanceState {
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
	return state
}

func convertInstance(instance *types.Instance) *kubecontainer.Pod {
	//instance name = namespace+"+"+name
	split := strings.Split(instance.Name, "+")
	namespace := split[0]
	name := split[1]
	image := getImageName(instance.ImageId, instance.Infrastructure)
	state := toContainerState(instance.State)
	hashInfo := hashable{
		state: state,
		namespace: namespace,
		name: name,
		image: image,
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
		Image: image,
		ImageID: instance.ImageId,
		Hash: hash(hashInfo),
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

func convertImage(image *types.Image) kubecontainer.Image {
	//unik stores the image, not the kubelet
	return kubecontainer.Image{
		ID: image.Id,
		Size: 1,
	}
}