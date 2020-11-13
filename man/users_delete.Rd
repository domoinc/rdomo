\name{users_delete}
\alias{users_delete}
\title{Delete a Domo user account.}
\usage{
users_delete(user_id)
}
\arguments{
\item{user_id}{Domo user id.}
}
\value{
No content is returned.
}
\description{
This function will delete one Domo user.

Note: even though the API documentation says this action cannot be reversed, that is not entirely accurate. A user account can be un-deleted by inviting a user with the same email address. This can be done using Domo's front-end or using the users_add function in this package.
}
\references{
API Documentation: \url{https://developer.domo.com/docs/users-api-reference/users-2#Delete\%20a\%20user}
}