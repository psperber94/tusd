package tusd_test

import (
	"github.com/psperber94/tusd"
	"github.com/psperber94/tusd/filestore"
	"github.com/psperber94/tusd/limitedstore"
	"github.com/psperber94/tusd/memorylocker"
)

func ExampleNewStoreComposer() {
	composer := tusd.NewStoreComposer()

	fs := filestore.New("./data")
	fs.UseIn(composer)

	ml := memorylocker.New()
	ml.UseIn(composer)

	ls := limitedstore.New(1024*1024*1024, composer.Core, composer.Terminater)
	ls.UseIn(composer)

	config := tusd.Config{
		StoreComposer: composer,
	}

	_, _ = tusd.NewHandler(config)
}
