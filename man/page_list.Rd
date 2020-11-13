\name{page_list}
\alias{page_list}
\title{Download list of pages}
\usage{
page_list(limit = -1, offset = 0)
}
\arguments{
\item{limit}{Limit number of results (default = -1). When limit < 0, all pages will be returned.}

\item{offset}{Offset when paging through results.}
}
\value{
List of pages.
}
\description{
Download list of pages. Note that subpages will be returned along with their parent, meaning more pages will be returned than might be expected.
}
\references{
API Documentation: \url{https://developer.domo.com/docs/page-api-reference/page#List\%20pages}
}