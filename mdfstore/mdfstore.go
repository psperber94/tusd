package mdfstore

import (
	"encoding/json"
	"fmt"
	"github.com/psperber94/tusd"
	"github.com/psperber94/tusd/uid"
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"os/exec"

	"gopkg.in/Acconut/lockfile.v1"
)

var defaultFilePerm = os.FileMode(0664)

// See the tusd.DataStore interface for documentation about the different
// methods.
type MdfStore struct {
	// Relative or absolute path to store files in. MdfStore does not check
	// whether the path exists, use os.MkdirAll in this case on your own.
	Path string
	ConverterPath string
}

// New creates a new file based storage backend. The directory specified will
// be used as the only storage entry. This method does not check
// whether the path exists, use os.MkdirAll to ensure.
// In addition, a locking mechanism is provided.
func New(uploadPath string, converterPath string) MdfStore {
	return MdfStore{uploadPath, converterPath}
}

// UseIn sets this store as the core data store in the passed composer and adds
// all possible extension to it.
func (store MdfStore) UseIn(composer *tusd.StoreComposer) {
	composer.UseCore(store)
	composer.UseGetReader(store)
	composer.UseTerminater(store)
	composer.UseFinisher(store)
	composer.UseLocker(store)
	composer.UseConcater(store)
	composer.UseLengthDeferrer(store)
}

func (store MdfStore) NewUpload(info tusd.FileInfo) (id string, err error) {
	id = uid.Uid()
	info.ID = id

	// Create .bin file with no content
	file, err := os.OpenFile(store.binPath(id), os.O_CREATE|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("upload directory does not exist: %s", store.Path)
		}
		return "", err
	}
	defer file.Close()

	// writeInfo creates the file by itself if necessary
	err = store.writeInfo(id, info)
	return
}

func (store MdfStore) WriteChunk(id string, offset int64, src io.Reader) (int64, error) {
	file, err := os.OpenFile(store.binPath(id), os.O_WRONLY|os.O_APPEND, defaultFilePerm)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	n, err := io.Copy(file, src)
	return n, err
}

func (store MdfStore) GetInfo(id string) (tusd.FileInfo, error) {
	info := tusd.FileInfo{}
	data, err := ioutil.ReadFile(store.infoPath(id))
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return info, err
	}

	stat, err := os.Stat(store.binPath(id))
	if err != nil {
		return info, err
	}

	info.Offset = stat.Size()

	return info, nil
}

func (store MdfStore) GetReader(id string) (io.Reader, error) {
	return os.Open(store.binPath(id))
}

func (store MdfStore) Terminate(id string) error {
	if err := os.Remove(store.infoPath(id)); err != nil {
		return err
	}
	if err := os.Remove(store.binPath(id)); err != nil {
		return err
	}
	return nil
}

func (store MdfStore) ConcatUploads(dest string, uploads []string) (err error) {
	file, err := os.OpenFile(store.binPath(dest), os.O_WRONLY|os.O_APPEND, defaultFilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, id := range uploads {
		src, err := store.GetReader(id)
		if err != nil {
			return err
		}

		if _, err := io.Copy(file, src); err != nil {
			return err
		}
	}

	return
}

func (store MdfStore) DeclareLength(id string, length int64) error {
	info, err := store.GetInfo(id)
	if err != nil {
		return err
	}
	info.Size = length
	info.SizeIsDeferred = false
	return store.writeInfo(id, info)
}

func (store MdfStore) LockUpload(id string) error {
	lock, err := store.newLock(id)
	if err != nil {
		return err
	}

	err = lock.TryLock()
	if err == lockfile.ErrBusy {
		return tusd.ErrFileLocked
	}

	return err
}

func (store MdfStore) UnlockUpload(id string) error {
	lock, err := store.newLock(id)
	if err != nil {
		return err
	}

	err = lock.Unlock()

	// A "no such file or directory" will be returned if no lockfile was found.
	// Since this means that the file has never been locked, we drop the error
	// and continue as if nothing happened.
	if os.IsNotExist(err) {
		err = nil
	}

	return err
}

// newLock contructs a new Lockfile instance.
func (store MdfStore) newLock(id string) (lockfile.Lockfile, error) {
	path, err := filepath.Abs(filepath.Join(store.Path, id+".lock"))
	if err != nil {
		return lockfile.Lockfile(""), err
	}

	// We use Lockfile directly instead of lockfile.New to bypass the unnecessary
	// check whether the provided path is absolute since we just resolved it
	// on our own.
	return lockfile.Lockfile(path), nil
}

// binPath returns the path to the .bin storing the binary data.
func (store MdfStore) binPath(id string) string {
	return filepath.Join(store.Path, id+".bin")
}

// infoPath returns the path to the .info file storing the file's info.
func (store MdfStore) infoPath(id string) string {
	return filepath.Join(store.Path, id+".info")
}

// writeInfo updates the entire information. Everything will be overwritten.
func (store MdfStore) writeInfo(id string, info tusd.FileInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(store.infoPath(id), data, defaultFilePerm)
}

func copyOutput(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
}

func (store MdfStore) FinishUpload(id string) error {
	fmt.Println("File Upload with Id: ", id, " finished")

	time.sleep(500 * time.Millisecond)

	converter := exec.Command("python",store.ConverterPath, store.binPath(id))
	stdout, err := converter.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderr, err := converter.StderrPipe()
	if err != nil {
		panic(err)
	}
	if err := converter.Start(); err != nil{
		return err
	}

	go copyOutput(stdout)
	go copyOutput(stderr)
	converter.Wait()
	return nil
}


