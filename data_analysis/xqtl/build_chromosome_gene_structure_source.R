# This script builds the `chromosome_features` and `gene_structure` JSON source
# files for the xQTL explorer, using information from the eQTL results files and
# Ensembl release 104.
#
# Steps:
#   1. Download Ensembl 104 GTF and database files
#   2. Download all eQTL subclass-level results files from Synapse
#      - Extract all unique variants and their locations from these files
#   3. Read and clean up Ensembl source data
#      - Map each gene to a single canonical transcript
#      - Subset the GTF data to protein-coding genes only, plus transcript-level
#        items (TES/TSS, exons, UTRs) from canonical transcripts only
#   4. Create the `chromosome_features` file (all protein-coding genes)
#      - Stack gene locations and variant locations into one data frame
#      - Create the nested `location` object for each item and write to JSON
#   5. Create the `gene_structure` file
#      5a. Further subset the GTF to only genes/transcript items that are
#          associated with a variant
#      5b. Extract transcription start site / transcription end site data
#          - TES/TSS label applied to start/end coordinates based on strand
#      5c. Extract exons
#      5d. Extract UTR data
#          - 5'/3' label applied based on strand
#      5e. Stack TSS/TES, exon, UTR, and variant data together
#          - Create the nested `item` object for each row
#          - Group all items together by gene and write to JSON
#
# JSON file structure for chromosome_features:
#   # Gene item
#   {
#     "type": "gene",
#     "ensembl_gene_id": "ENSG00000000419",
#     "hgnc_symbol": "DPM1",
#     "strand": "negative", # "positive" or "negative"
#     "location": { "chromosome": "20", "start": 50934867, "end": 50959140 }
#   }
#   # Variant item
#   {
#     "type": "variant",
#     "variant_id": "rs112618790",
#     "location": { "chromosome": "1", "start": 841851, "end": 841852 }
#   }
#
# JSON file structure for gene_structure:
# {
#   "gene_symbol": "DPM1",
#   "ensembl_gene_id": "ENSG00000000419",
#   "strand": "negative", # "positive" or "negative"
#   "items": [
#     { "type": "transcription-start-site", "start": 50958532, "end": 50958532 },
#     { "type": "transcription-end-site", "start": 50934870, "end": 50934870 },
#     { "type": "utr", "utr_type": "5'", "start": 50934870, "end": 50935131 },
#     { "type": "utr", "utr_type": "3'", "start": 50958524, "end": 50958532 },
#     { "type": "exon", "start": 50958363, "end": 50958532 },
#     ... # Any number of exons
#     { "type": "variant", "variant_id": "rs75765064", "start": 50895416, "end": 50895417 },
#     ... # Any number of variants
#   ]
# }

library(rtracklayer)
library(dplyr)
library(stringr)
library(synapser)
library(jsonlite)

dir_input <- file.path("data_analysis", "xqtl", "input")
dir_output <- file.path("data_analysis", "xqtl", "output")

# 1. Download Ensembl source files ---------------------------------------------

# Ensembl 104 archive

# GTF file used for alignment
gtf_url <- "https://ftp.ensembl.org/pub/release-104/gtf/homo_sapiens/Homo_sapiens.GRCh38.104.gtf.gz"
gtf_file <- file.path(dir_input, basename(gtf_url))

# List of genes and their canonical transcripts
gene_info_url <- "https://ftp.ensembl.org/pub/release-104/mysql/homo_sapiens_core_104_38/gene.txt.gz"
gene_info_file <- file.path(dir_input, basename(gene_info_url))

# List of transcripts, used to translate numeric transcript IDs from the
# gene_info_file to the Ensembl transcript ID
transcript_info_url <- "https://ftp.ensembl.org/pub/release-104/mysql/homo_sapiens_core_104_38/transcript.txt.gz"
transcript_info_file <- file.path(dir_input, basename(transcript_info_url))

if (!file.exists(gtf_file)) {
  download.file(gtf_url, gtf_file, method = "curl")
}

if (!file.exists(gene_info_file)) {
  download.file(gene_info_url, gene_info_file, method = "curl")
}

if (!file.exists(transcript_info_file)) {
  download.file(transcript_info_url, transcript_info_file, method = "curl")
}


# 2. Download eQTL subclass results files --------------------------------------

synLogin(silent = TRUE)

# Only download and read the files that end in "with_pvalue"
all_files <- synGetChildren("syn78321269", includeTypes = list("file"))$asList()
file_names <- sapply(all_files, "[[", "name")
ids <- sapply(all_files[grepl("_with_pvalue", all_files)], "[[", "id")

# Check: half of the files in 'all_files' should be 'with_pvalue' files.
stopifnot(length(ids) == length(all_files) / 2)

# This will take several minutes: there are ~500 files to download. Each file
# has different subsets of all possible variants, so we need to read every file
# to get the full set of variants for each chromosome.
results_files <- lapply(ids, synGet, downloadLocation = dir_input)
file_paths <- sapply(results_files, "[[", "path")

chromosomes <- str_extract(file_paths, "chr[0-9]+") |> unique()

# To reduce memory usage, we read the files for one chromosome at a time and
# subset to unique values before moving to the next chromosome
variants <- lapply(chromosomes, function(chr) {
  print(str_glue("Extracting variants for {chr}"))

  chrom_files <- file_paths[grepl(str_glue("_{chr}_"), file_paths)]
  chr_data <- lapply(chrom_files, function(file) {
    # Files are g-zipped tsv files with no column header
    data <- read.table(gzfile(file), sep = "\t") |>
      select(V1:V4, V7)
    colnames(data) <- c("chromosome", "start", "end", "variant_id", "hgnc_symbol")
    return(data)
  })

  chr_data <- do.call(rbind, chr_data) |> distinct()
  print(str_glue(
    "...{length(unique(chr_data$variant_id))} unique variants found."
  ))

  return(chr_data)
})

variants <- do.call(rbind, variants) |>
  mutate(chromosome = as.character(chromosome),
         type = "variant")


# 3. Read Ensembl source files -------------------------------------------------

# The GTF file doesn't contain information about which transcripts are the
# canonical transcript for each gene, so we join data from the Ensembl database.
# Need to read these files in with fread() because read.table can't handle mySQL
# encoding in the file.
gene_info <- data.table::fread(gene_info_file) |>
  as.data.frame() |>
  dplyr::rename(internal_transcript_id = V12,
                gene_id = V13)

transcript_info <- data.table::fread(transcript_info_file) |>
  as.data.frame() |>
  dplyr::rename(internal_transcript_id = V1,
                transcript_id = V14)

canonical_transcripts <- merge(gene_info, transcript_info,
                               by = "internal_transcript_id") |>
  select(gene_id, transcript_id)

# Check: There should be 1 canonical transcript per gene, and no duplicates
# across genes
stopifnot(nrow(distinct(canonical_transcripts)) == nrow(canonical_transcripts))
stopifnot(all(!duplicated(canonical_transcripts$gene_id)))
stopifnot(all(!duplicated(canonical_transcripts$transcript_id)))

# GTF for Ensembl release 104
gtf <- rtracklayer::import(gtf_file, format = "gtf") |>
  # Remove pseudogenes and mitochondrial rRNA and tRNA
  #subset(!grepl("pseudogene", gene_biotype)) |>
  #subset(!grepl("^Mt_", gene_biotype)) |>

  # Subset to genes, or transcript-related items from canonical transcripts only
  subset(type == "gene" |
    transcript_id %in% canonical_transcripts$transcript_id)

# Rename some fields
gtf$ensembl_gene_id <- gtf$gene_id
gtf$hgnc_symbol <- gtf$gene_name
gtf$chromosome <- as.character(seqnames(gtf))

# Check: all genes we care about should be in canonical_transcripts
stopifnot(all(gtf$gene_id %in% canonical_transcripts$gene_id))

# Check: all gene symbols in "variants" should exist in the GTF
stopifnot(all(unique(variants$hgnc_symbol) %in% unique(gtf$hgnc_symbol)))

gtf_cols <- c(
  "chromosome", "ensembl_gene_id", "hgnc_symbol", "strand", "start", "end"
)


# 4. Chromosome features JSON --------------------------------------------------

# Gene locations
genes_df <- subset(gtf, type == "gene") |>
  as.data.frame() |>
  select(all_of(gtf_cols), type) |>
  mutate(strand = ifelse(strand == "+", "positive", "negative"))

# Unique variants, ignoring potential duplicates associated with multiple genes
var_df <- variants |>
  select(-hgnc_symbol) |>
  distinct()

# Concatenate genes and variants -- list_rbind will add all columns from both
# data frames and fill with NA where needed. When a row has an NA value for a
# given column, write_json will skip that column when writing the entry, so
# only the information defined for each gene/variant ends up in the JSON.
chr_final <- purrr::list_rbind(list(genes_df, var_df))

# Creating a "location" column where each item is a data frame will cause
# write_json to write it as a nested structure.
chr_final$location <- select(chr_final, chromosome, start, end)

chr_final <- arrange(chr_final, chromosome, start, end) |>
  select(-chromosome, -start, -end) |>
  # Put "type" first
  relocate(type)

write_json(chr_final,
  file.path(dir_output, "chromosome_features.json"),
  pretty = TRUE
)


# 5. Gene structure file -------------------------------------------------------

# 5a: The gene structure file will contain info from canonical transcripts only,
# from only the genes that have associated variants
# WARNING: the variants df has ~700 symbols that aren't present in the GTF
gtf <- subset(gtf, hgnc_symbol %in% variants$hgnc_symbol)


## 5b. TSS and TES -------------------------------------------------------------

# TSS should be start coord of transcript on + strand, end coord on - strand
# TES should be end coord of transcript on + strand, start coord on - strand
transcripts <- subset(gtf, type == "transcript")

# Check: There should be exactly one row per transcript
stopifnot(length(transcripts) == length(unique(transcripts$transcript_id)))

tss_df <- as.data.frame(transcripts) |>
  select(all_of(gtf_cols)) |>

  # Move start and end columns into long format
  tidyr::pivot_longer(
    cols = c(start, end),
    names_to = "type",
    values_to = "start"
  ) |>

  # Add end coordinate (equal to start coordinate), and add TSS/TES type based
  # on strand
  mutate(
    end = start,
    type = case_when(
      type == "start" & strand == "+" ~ "transcription-start-site",
      type == "start" & strand == "-" ~ "transcription-end-site",
      type == "end" & strand == "+" ~ "transcription-end-site",
      type == "end" & strand == "-" ~ "transcription-start-site",
      .default = NULL
    )
  )

# Check: There shouldn't be any NULL types
stopifnot(all(!is.null(tss_df$type)))


## 5c. Exons -------------------------------------------------------------------

exons <- subset(gtf, type == "exon")

# Check: there shouldn't be any overlapping exon regions in a given gene
exons_by_gene <- split(exons, exons$gene_id)
stopifnot(all(lengths(exons_by_gene) == lengths(reduce(exons_by_gene))))

exons_df <- as.data.frame(exons) |>
  select(all_of(gtf_cols), type)


## 5d. UTRs --------------------------------------------------------------------

utrs <- subset(gtf, type %in% c("five_prime_utr", "three_prime_utr"))

# Check: there shouldn't be any overlapping UTR regions
utrs_by_gene <- split(utrs, utrs$gene_id)
stopifnot(all(lengths(utrs_by_gene) == lengths(reduce(utrs_by_gene))))

# Although the UTRs are labeled in the GTF as 5' or 3', they are labeled
# relative to the positive strand. The labels need to be swapped for negative
# strand genes.
utrs_df <- as.data.frame(utrs) |>
  select(all_of(gtf_cols), type) |>
  mutate(
    utr_type = case_when(
      type == "five_prime_utr" & strand == "+" ~ "5'",
      type == "five_prime_utr" & strand == "-" ~ "3'",
      type == "three_prime_utr" & strand == "+" ~ "3'",
      type == "three_prime_utr" & strand == "-" ~ "5'",
      .default = NULL
    ),
    type = "utr"
  )

# Check: there shouldn't be any NULL utr_types
stopifnot(all(!is.null(utrs_df$utr_type)))


## 5e. Write to JSON -----------------------------------------------------------

# Same as above, all columns are combined but NA values are skipped during
# write_json, so each type of data will have a different set of keys in the JSON.
gene_struct <- purrr::list_rbind(list(tss_df, exons_df, utrs_df)) |>
  mutate(strand = ifelse(strand == "+", "positive", "negative"))

# Pull Ensembl gene IDs into the variants df -- will also remove any variants
# whose gene symbol does not match what is in the GTF
var_struct <- merge(variants,
                    select(genes_df, ensembl_gene_id, hgnc_symbol))

# Create the "item" structure as a column of data frames so it is written as
# a nested structure
gene_struct_final <- purrr::list_rbind(list(gene_struct, var_struct))
gene_struct_final$item <- select(
  gene_struct_final, type, utr_type, variant_id, start, end
)

# Combine into one entry per gene
gene_struct_final <- gene_struct_final |>
  select(ensembl_gene_id, hgnc_symbol, strand, item) |>
  group_by(ensembl_gene_id, hgnc_symbol) |>
  summarize(
    strand = unique(na.omit(strand)),
    items = list(item),
    .groups = "drop"
  ) |>
  arrange(ensembl_gene_id)

write_json(gene_struct_final,
  file.path(dir_output, "gene_structure.json"),
  pretty = TRUE
)
