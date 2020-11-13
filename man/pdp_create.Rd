\name{pdp_create}
\alias{pdp_create}
\title{Create a new PDP policy}
\usage{
pdp_create(ds, policy_def)
}
\arguments{
\item{ds}{Data set id from Domo.}

\item{policy_def}{List object with parameters as given in documentation.}
}
\value{
List with all policy properties plus the id assigned by Domo.
}
\description{
This function creates a new PDP policy on the given data set. The PDP policy is constructed as a list object and the function will convert that list to a JSON string before creating the policy.
}
\references{
API Documentation: \url{https://developer.domo.com/docs/dataset-api-reference/dataset#Create\%20a\%20Personalized\%20Data\%20Permission\%20(PDP)\%20policy}
}