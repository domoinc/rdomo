\name{users_list}
\alias{users_list}
\title{List all users in Domo instance.}
\usage{
users_list(df_output = TRUE, batch_size = 500)
}
\arguments{
\item{df_output}{If TRUE, function will return data in a dataframe or tibble. If FALSE, the raw list output will be returned.}
}
\value{
List of all users in Domo.
}
\description{
This function returns a list of all users in the specified Domo instance. The API requires the user to page through results. Paging is not necessary with this function.
}
\references{
API Documentation: \url{https://developer.domo.com/docs/users-api-reference/users-2#List\%20users}
}