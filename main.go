package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/OliverCardiff/sentromap_core3/kmers"
	"github.com/OliverCardiff/sentromap_core3/processing"
)

func tellSubs() {

	println()
	println("  ~~ Sentromap Data Construction Tools ~~")
	println()
	println(" subcommands:")
	println()

	println(" - extract -  Extracts primary construction data from a genome sequence (fasta)")
	println(" - trie    -  Convert a construction file to a trie->position index")
	println(" - extrap  -  Extrapolates a N-variant entropic cascade from a trie")
	println()

	os.Exit(1)
}

func main() {

	thrs := runtime.NumCPU()

	extractCmd := flag.NewFlagSet("extract", flag.ExitOnError)
	extrapCmd := flag.NewFlagSet("extrap", flag.ExitOnError)
	trieCmd := flag.NewFlagSet("trie", flag.ExitOnError)

	threadsExtract := extractCmd.Int("t", thrs, "number of extraction threads")
	fileExtract := extractCmd.String("i", "", "fasta file containing genome of interest")
	tmpExtract := extractCmd.String("tmp", "tmp", "temporary folder location")
	outputExtract := extractCmd.String("o", "", "output destination - a new folder will be created here with construction files in it")

	threadsTrie := trieCmd.Int("t", thrs, "number of trie construction threads")
	fileTrie := trieCmd.String("i", "", "primary k-mer all-project union database")
	outputTrie := trieCmd.String("o", "", "sentromap trie index file")

	threadsExtr := extrapCmd.Int("t", thrs, "number of extrapolations threads")
	trieExtr := extrapCmd.String("i", "", "trie data structure to extrapolate from")
	resultExtr := extrapCmd.String("o", "", "data file that results will be written to")

	if len(os.Args) < 2 {
		tellSubs()
	}

	var err error
	switch os.Args[1] {
	case "extract":
		extractCmd.Parse(os.Args[2:])
		if *fileExtract == "" || *outputExtract == "" {
			log.Println("You need to supply -i and -o args")
			tellSubs()
		}
		fmt.Print("\n  ~~  SENTROMAP EXTRACTION ROUTINE  ~~  \n\n")
		err = kmers.GenomeToKset(*fileExtract, *outputExtract, *tmpExtract, *threadsExtract)

	case "trie":
		trieCmd.Parse(os.Args[2:])
		err = processing.Trie(*fileTrie, *outputTrie, *threadsTrie)
	case "extrap":
		extrapCmd.Parse(os.Args[2:])
		err = processing.Extrap(*trieExtr, *resultExtr, *threadsExtr)

	default:
		tellSubs()
	}

	if err != nil {
		log.Fatal(err)
	}
}
