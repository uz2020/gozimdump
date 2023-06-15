package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	zim "github.com/akhenakh/gozim"
	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/analysis/lang/en"
	_ "github.com/blevesearch/bleve/analysis/lang/fr"

	_ "github.com/blevesearch/bleve/index/store/goleveldb"
)

type ArticleIndex struct {
	Title   string
	Content string
}

var (
	path         = flag.String("path", "", "path for the zim file")
	indexPath    = flag.String("index", "", "path for the index directory")
	cpuprofile   = flag.String("cpuprofile", "", "write cpu profile to file")
	z            *zim.ZimReader
	lang         = flag.String("lang", "", "language for indexation")
	batchSize    = flag.Int("batchsize", 1000, "size of bleve batches")
	indexContent = flag.Bool("content", false, "expermintal: index the content of the page")
)

// Type return the Article type (used for bleve indexer)
func (a *ArticleIndex) Type() string {
	return "Article"
}

func main() {
	bleve.Config.DefaultKVStore = "goleveldb"

	flag.Parse()

	if *path == "" {
		log.Fatal("provide a zim file path")
	}

	z, err := zim.NewReader(*path, false)
	if err != nil {
		log.Fatal(err)
	}

	mapping := bleve.NewIndexMapping()
	mapping.DefaultType = "Article"

	articleMapping := bleve.NewDocumentMapping()
	mapping.AddDocumentMapping("Article", articleMapping)

	fieldMapping := bleve.NewTextFieldMapping()
	fieldMapping.Store = false
	fieldMapping.Index = true
	fieldMapping.Analyzer = "standard"

	switch *lang {
	case "fr":
		fieldMapping.Analyzer = "frnostemm"
	case "en":
		fieldMapping.Analyzer = "ennostemm"
	case "ar", "ca", "ckb", "el", "eu", "gl", "hy", "in", "ja", "bg", "cjk", "cs", "fa", "ga", "hi", "id", "it", "pt":
		fieldMapping.Analyzer = *lang

	case "":

	default:
		log.Fatal("unsupported language")
	}

	articleMapping.AddFieldMappingsAt("Title", fieldMapping)
	if *indexContent {
		articleMapping.AddFieldMappingsAt("Content", fieldMapping)
	}

	i := 0

	divisor := float64(z.ArticleCount) / 100

	f, err := os.OpenFile("data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	z.ListTitlesPtrIterator(func(idx uint32) {
		a, err := z.ArticleAtURLIdx(idx)
		if err != nil || a.EntryType == zim.DeletedEntry {
			i++
			return
		}

		if a.Namespace == 'C' {
			if a.MimeType() == "text/html" {
				fmt.Println(a.FullURL())
				d, err := a.Data()
				if err != nil {
					log.Fatal(err.Error())
				}

				os.WriteFile(a.FullURL(), d, 0644)
				if _, err := f.Write(d); err != nil {
					log.Fatal(err.Error())
				}
			}
		}

		if i%*batchSize == 0 {
			fmt.Printf("%.2f%% done\n", float64(i)/divisor)
		}

		i++
	})
}
