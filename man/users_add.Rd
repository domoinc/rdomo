\name{users_add}
\alias{users_add}
\title{Add a user or users}
\usage{
users_add(x_name, x_email, x_role, x_sendInvite = FALSE)
}
\arguments{
\item{x_name}{User's name}

\item{x_email}{Users's email address}

\item{x_role}{Users's Domo role (one of Admin, Privileged, Participant). Role must match exactly, otherwise the API will throw an error.}

\item{x_sendInvite}{TRUE if you want an email sent or FALSE if not. Defaults to FALSE}
}
\value{
Full User record from API.
}
\description{
This function will create exactly one user in the object's instance. To create multiple users, call this function in a list or in an apply function (preferrably from an apply function).
}
\references{
API Documentation: \url{https://developer.domo.com/docs/users-api-reference/users-2#Create\%20a\%20user}
}