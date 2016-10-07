package mux

import (
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/api"
	kubetypes "k8s.io/kubernetes/pkg/types"

	"io"
	"k8s.io/kubernetes/pkg/util/flowcontrol"
	"k8s.io/kubernetes/pkg/util/term"
	"github.com/emc-advanced-dev/pkg/errors"
	"strings"
	"k8s.io/kubernetes/pkg/kubelet/unik"
)

var unikProviders = []string{
	"AWS",
	"OPENSTACK",
	"VIRTUALBOX",
	"QEMU",
	"XEN",
}

type Runtime struct {
	docker kubecontainer.Runtime
	unik   kubecontainer.Runtime
}

func New(docker, unik kubecontainer.Runtime) *Runtime {
	return &Runtime{
		docker: docker,
		unik: unik,
	}
}

// Type returns the type of the container runtime.
func (r *Runtime) Type() string {
	return r.docker.Type()
}

// Version returns the version information of the container runtime.
func (r *Runtime) Version() (kubecontainer.Version, error) {
	return r.docker.Version()
}

// APIVersion returns the cached API version information of the container
// runtime. Implementation is expected to update this cache periodically.
// This may be different from the runtime engine's version.
func (r *Runtime) APIVersion() (kubecontainer.Version, error) {
	return r.Version()
}

// Status returns error if the runtime is unhealthy; nil otherwise.
func (r *Runtime) Status() error {
	return r.docker.Status()
}

// GetPods returns a list of containers grouped by pods. The boolean parameter
// specifies whether the runtime returns all containers including those already
// exited and dead containers (used for garbage collection).
func (r *Runtime) GetPods(all bool) ([]*kubecontainer.Pod, error) {
	pods := []*kubecontainer.Pod{}
	ps, err := r.docker.GetPods(all)
	if err != nil {
		return nil, errors.New("getting containers from " + r.docker.Type(), err)
	}
	pods = append(pods, ps...)
	ps, err = r.unik.GetPods(all)
	if err != nil {
		return nil, errors.New("getting containers from " + r.unik.Type(), err)
	}
	pods = append(pods, ps...)
	return pods, nil
}

// GarbageCollect removes dead containers using the specified container gc policy
// If allSourcesReady is not true, it means that kubelet doesn't have the
// complete list of pods from all avialble sources (e.g., apiserver, http,
// file). In this case, garbage collector should refrain itself from aggressive
// behavior such as removing all containers of unrecognized pods (yet).
func (r *Runtime) GarbageCollect(gcPolicy kubecontainer.ContainerGCPolicy, allSourcesReady bool) error {
	if err := r.docker.GarbageCollect(gcPolicy, allSourcesReady); err != nil {
		return errors.New("garbage collecting from " + r.docker.Type(), err)
	}
	if err := r.unik.GarbageCollect(gcPolicy, allSourcesReady); err != nil {
		return errors.New("garbage collecting from " + r.unik.Type(), err)
	}
	return nil
}

// Syncs the running pod into the desired pod.
func (r *Runtime) SyncPod(desiredPod *api.Pod, desiredPodStatus api.PodStatus, internalPodStatus *kubecontainer.PodStatus, pullSecrets []api.Secret, backOff *flowcontrol.Backoff) (result kubecontainer.PodSyncResult) {
	if isUnikPod(desiredPod.Namespace) {
		return r.unik.SyncPod(desiredPod, desiredPodStatus, internalPodStatus, pullSecrets, backOff)
	}
	return r.docker.SyncPod(desiredPod, desiredPodStatus, internalPodStatus, pullSecrets, backOff)
}

// KillPod kills all the containers of a pod. Pod may be nil, running pod must not be.
// gracePeriodOverride if specified allows the caller to override the pod default grace period.
// only hard kill paths are allowed to specify a gracePeriodOverride in the kubelet in order to not corrupt user data.
// it is useful when doing SIGKILL for hard eviction scenarios, or max grace period during soft eviction scenarios.
func (r *Runtime) KillPod(pod *api.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) error {
	if isUnikPod(runningPod.Namespace) {
		return r.unik.KillPod(pod, runningPod, gracePeriodOverride)
	}
	return r.docker.KillPod(pod, runningPod, gracePeriodOverride)
}

// GetPodStatus retrieves the status of the pod, including the
// information of all containers in the pod that are visble in Runtime.
func (r *Runtime) GetPodStatus(uid kubetypes.UID, name, namespace string) (*kubecontainer.PodStatus, error) {
	if isUnikPod(namespace) {
		return r.unik.GetPodStatus(uid, name, namespace)
	}
	return r.docker.GetPodStatus(uid, name, namespace)
}

// PullImage pulls an image from the network to local storage using the supplied
// secrets if necessary.
func (r *Runtime) PullImage(image kubecontainer.ImageSpec, pullSecrets []api.Secret) error {
	if isUnikImage(image.Image) {
		return r.unik.PullImage(image, pullSecrets)
	}
	return r.docker.PullImage(image, pullSecrets)
}

// IsImagePresent checks whether the container image is already in the local storage.
func (r *Runtime) IsImagePresent(image kubecontainer.ImageSpec) (bool, error) {
	if isUnikImage(image.Image) {
		return r.unik.IsImagePresent(image)
	}
	return r.docker.IsImagePresent(image)
}

// Gets all images currently on the machine.
func (r *Runtime) ListImages() ([]kubecontainer.Image, error) {
	images := []kubecontainer.Image{}
	ims, err := r.docker.ListImages()
	if err != nil {
		return nil, errors.New("getting images from " + r.docker.Type(), err)
	}
	images = append(images, ims...)
	ims, err = r.unik.ListImages()
	if err != nil {
		return nil, errors.New("getting images from " + r.unik.Type(), err)
	}
	images = append(images, ims...)
	return images, nil
}

// Removes the specified image.
func (r *Runtime) RemoveImage(image kubecontainer.ImageSpec) error {
	if isUnikImage(image.Image) {
		return r.unik.RemoveImage(image)
	}
	return r.docker.RemoveImage(image)
}

// Returns Image statistics.
func (r *Runtime) ImageStats() (*kubecontainer.ImageStats, error) {
	return r.docker.ImageStats()
}

// Returns the filesystem path of the pod's network namespace; if the
// runtime does not handle namespace creation itself, or cannot return
// the network namespace path, it should return an error.
// by all containers in the pod.
func (r *Runtime) GetNetNS(containerID kubecontainer.ContainerID) (string, error) {
	if isUnikContainer(containerID) {
		return r.unik.GetNetNS(containerID)
	}
	return r.docker.GetNetNS(containerID)
}

// Returns the container ID that represents the Pod, as passed to network
// plugins. For example, if the runtime uses an infra container, returns
// the infra container's ContainerID.
func (r *Runtime) GetPodContainerID(pod *kubecontainer.Pod) (kubecontainer.ContainerID, error) {
	if isUnikPod(pod.Namespace) {
		return r.unik.GetPodContainerID(pod)
	}
	return r.docker.GetPodContainerID(pod)
}

// GetContainerLogs returns logs of a specific container. By
// default, it returns a snapshot of the container log. Set 'follow' to true to
// stream the log. Set 'follow' to false and specify the number of lines (e.g.
// "100" or "all") to tail the log.
func (r *Runtime) GetContainerLogs(pod *api.Pod, containerID kubecontainer.ContainerID, logOptions *api.PodLogOptions, stdout, stderr io.Writer) (err error) {
	if isUnikPod(pod.Namespace) {
		return r.unik.GetContainerLogs(pod, containerID, logOptions, stdout, stderr)
	}
	return r.docker.GetContainerLogs(pod, containerID, logOptions, stdout, stderr)
}

// Delete a container. If the container is still running, an error is returned.
func (r *Runtime) DeleteContainer(containerID kubecontainer.ContainerID) error {
	if isUnikContainer(containerID) {
		return r.unik.DeleteContainer(containerID)
	}
	return r.docker.DeleteContainer(containerID)
}

func (r *Runtime) AttachContainer(id kubecontainer.ContainerID, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan term.Size) (err error) {
	if isUnikContainer(id) {
		return r.unik.AttachContainer(id, stdin, stdout, stderr, tty, resize)
	}
	return r.docker.AttachContainer(id, stdin, stdout, stderr, tty, resize)
}

func (r *Runtime) ExecInContainer(containerID kubecontainer.ContainerID, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan term.Size) error {
	if isUnikContainer(containerID) {
		return r.unik.ExecInContainer(containerID, cmd, stdin, stdout, stderr, tty, resize)
	}
	return r.docker.ExecInContainer(containerID, cmd, stdin, stdout, stderr, tty, resize)
}

// Forward the specified port from the specified pod to the stream.
func (r *Runtime) PortForward(pod *kubecontainer.Pod, port uint16, stream io.ReadWriteCloser) error {
	if isUnikPod(pod.Namespace) {
		return r.unik.PortForward(pod, port, stream)
	}
	return r.docker.PortForward(pod, port, stream)
}

func isUnikPod(namespace string) bool {
	return namespace == "unik"
}

func isUnikImage(imageName string) bool {
	for _, provider := range unikProviders {
		if strings.Contains(strings.ToUpper(imageName), provider) {
			return true
		}
	}
	return false
}

func isUnikContainer(containerID kubecontainer.ContainerID) bool {
	return containerID.Type == unik.Type_Unik
}