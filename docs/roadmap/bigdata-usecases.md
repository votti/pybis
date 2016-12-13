# Summary

openBIS should be extended to support big-data use cases. This requires changing openBIS to be aware of data that it does not manage (externally managed data). Interaction with openBIS for these use cases will be done with a command-line client.

# Implementation Breakdown

- Implement command-line tool (obis) for interacting with openBIS
- Extend openBIS to support externally managed data

# obis Commands

| Command       | Description                                                                                 |
|---------------|---------------------------------------------------------------------------------------------|
| obis init     | Register externally managed data set with openBIS                                                    |
| obis commit   | Commit local state to openBIS server                                                        |
| obis add-ref  | Reference a existing externally managed data set                                                     |
| obis clone    | Copy data to new location and register copy with openBIS                                    |
| obis get      | Retrieve the files for an externally managed data set                                                |


# Implementation Sketch

Our suggestion is to implement this feature by leveraging existing tools that can help us. In particular, our suggestion is to implement the obis with help from *git* and *git-annex*. Unmanaged obis data would be kept in the form of a git repository. Large files would be stored using git-annex, which is an extension of git explicitly designed for this purpose.

In this implementation openBIS would neither store files nor file metadata for externally managed data sets. It would only store git commit IDs and URLs for git repositories. Git itself would be used to make the contents of externally managed data sets available to others. The advantages of this solution include the ability to publish workflows while keeping some data private, as might be necessary in, e.g., personalized medicine contexts.

The solution we propose here has many similarities to the one outlined in this paper:

    V. Korolev, A. Joshi, V. Korolev, M.A. Grasso, A. Joshi, M.A. Grasso, et al., "PROB: A tool for tracking provenance and reproducibility of big data experiments", Reproduce'14. HPCA 2014, vol. 11, pp. 264-286, 2014.
    http://ebiquity.umbc.edu/_file_directory_/papers/693.pdf

# Deployment Prerequisites

- git
- git-annex
- python 3

# Example Use Case

A user wishes to analyze a large data set on a cluster. Here is an overview of the commands she would execute and their semantics.

| Command              | Effect                                                                                  |
|----------------------|-----------------------------------------------------------------------------------------|
| [download data]      | Stage data on the server in folder "foo"                                                |
| obis init data .     | Prepare an externally managed (data) data set                                                    |
| obis commit          | Inform the openBIS server about the current state                                       |
| mkdir/cd ../bar      | Create a folder to contain the analysis code                                            |
| obis init analysis . | Prepare an externally managed (analysis) data set                                                |
| obis add-ref  [path] | Indicate that the analysis data set references the data data set                        |
| [run analysis]       | Do the data analysis                                                                    |
| obis commit          | Commit the results of the analysis to openBIS.                                          |

This would result in two data sets created in openBIS: DS1 and DS2. DS1 would be an externally managed data data set and include as metadata the commit id for the data repository. DS2 would be an externally managed analysis data set and include as metadata the commit id for the analysis repository.

Later, the user could run the following sequence of commands on a different machine, for example.

| Command              | Effect                                                                                  |
|----------------------|-----------------------------------------------------------------------------------------|
| obis clone DS2-id    | Create a "clone" of DS2 locally. This also clones DS1 because it is referenced by DS2   |
| cd [ds1-dir]         | Enter the DS1 directory to run a command                                                |
| obis get .           | Retreive the files for this data set                                                    |
| [run analysis]       | Do the data analysis                                                                    |

This would make it possible to re-run the analysis on different infrastructure. Naturally, the results could be committed into openBIS afterwards.

# Commands

## obis init [data/analysis]

Create a new externally managed data set in openBIS. This command has two variants: data and analysis. With the data argument, a git-annex is also initialized so that the (potentially large) data files can be managed. With the analysis argument only git is initialized, since the repository is assumed to hold just source code and analysis results (which are assumed to be small).

## obis commit

Informs openBIS about the current state of the repository. If it is unknown to openBIS, a new data set is created. If is is known to openBIS, a new data set is created which is the child of the previous state of the data set. The externally managed data set stores the git commit id as metadata. Unmanaged data sets may have copies.

## obis add-ref [path do data set]

Store a reference to another data set. This is, for example, used in analysis data sets to refer to data data sets. This information allows obis to track state and, for example, notify if the data repository has uncommitted changes when the user tries to commit an analysis repository. It is also used to create the appropriate links between data sets in openBIS.

## obis clone [data set id]

Clone a data set that is known to openBIS. This create a "copy" data set in openBIS.

## obis get

Retrieve any data from the annex and save it locally.

# Handling of Scenarios

## Data analysis

This is described in the use case above.

## Visibility / Access / Publishing

The contents of externally managed data sets are not visible in general to openBIS users because they may reside on computers where the openBIS has no access. To make the contents visible, they need to be pushed to publicly accessible repositories. This gives users control of what data and code are public, and what are kept private. It also allows for partial publication of data, useful e.g., when dealing with privacy-sensitive content (say, in the context of medicine).

## Management of Data in Non-POSIX File Systems

This needs to be looked into in greater detail. Git-annex can manage data that is referenced by a URL, something that, e.g., HDFS can provide. Whether this satisfies all our needs is not yet known and needs to be briefly investigated.

# openBIS changes

- New data set type -- externally managed
- New concept on data sets: copies
- [Copies can be "altered"] -- This is no longer needed


# Outstanding Questions

- Korolev et. al. use git tree IDs instead of commit IDs. Are these better?
- How well does git-annex handle content in HDFS? Is some work necessary to improve this support?
