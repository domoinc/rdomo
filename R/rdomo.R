
DomoUtilities <- setRefClass("DomoUtilities",
	fields=list(client_id='ANY',secret='ANY',domain='ANY',scope='ANY',access_time='POSIXct',api_env='ANY'),
	methods=list(
		initialize=function(client_id=NA,secret=NA,domain=NA,scope=c('data'),access_time=as.POSIXct(NA)){
			if( is.na(client_id) & Sys.getenv("RDOMO_CLIENTID") != '' ){
				client_id <<- Sys.getenv("RDOMO_CLIENTID")
			}else if( !is.na(client_id) ) {
				client_id <<- client_id
			}else{
				stop('No Client ID Provided')
			}

			if( is.na(secret) & Sys.getenv("RDOMO_SECRET") != '' ){
				secret <<- Sys.getenv("RDOMO_SECRET")
			}else if( !is.na(secret) ){
				secret <<- secret
			}else{
				stop('No Secret Provided')
			}

			if( is.na(domain) & Sys.getenv('RDOMO_DOMAIN') != '' ){
				domain <<- Sys.getenv('RDOMO_DOMAIN')
			}else if( !is.na(domain) ){
				domain <<- domain
			}else{
				domain <<- 'api.domo.com'
			}
			scope <<- scope
			access_time <<- access_time
			api_env <<- new.env()
		},
		set_access=function(){
			auth64 <- RCurl::base64(paste(.self$client_id,.self$secret,sep=':'))[[1]]
			my_headers <- httr::add_headers(c(Authorization=paste('Basic',auth64,sep=' ')))

			access <- httr::content(httr::GET(paste('https://',.self$domain,'/oauth/token',sep=''),my_headers,query=list(grant_type='client_credentials')))
			
			return_value <- 0
			if( !is.null(access$access_token) ){
				assign('access', access$access_token, .self$api_env)
				access_time <<- Sys.time()
				return_value <- 1
			}
			
			if( !is.null(access$status) ){
				stop('Access error: ',access$status,' ',access$message)
			}

			return(access)

		},
		get_access=function(){

			out <- 0

			if( is.na(.self$access_time) ){
				.self$set_access()
			}

			time_since <- as.numeric(difftime(Sys.time(),.self$access_time,units='mins'))

			if( time_since < 50 ){
				out <- get('access',.self$api_env)
			}else{
				.self$set_access()
				out <- get('access',.self$api_env)
			}

			return(out)
		},
		domo_content=function(response_object,success_code=200){
			out <- httr::content(response_object)
			if( response_object$status != success_code ){
				warning('\nAPI returned status: ',response_object$status,'. \nWith message: ',httr::content(response_object)$message)
			}
			return(out)
		},
		stream_create=function(up_ds,name,description,updateMethod,keyColumnNames){
			df_up <- as.data.frame(up_ds)

			dataframe_schema <- .self$schema_definition(df_up)
			json <- list(dataSet=list(name=name, description=description, schema=list(columns=dataframe_schema$columns)), updateMethod=updateMethod)
			if( updateMethod == 'UPSERT' & class(keyColumnNames) == 'character' & max(is.na(keyColumnNames)) == 0 ){
			    json$keyColumnNames <- keyColumnNames
			}
			body <- rjson::toJSON(json)

			headers <- httr::add_headers(c("Content-Type"='application/json', Accept='application/json', Authorization=paste('bearer',.self$get_access(), sep=' ')))
			url <- paste('https://',.self$domain,'/v1/streams', sep='')

			response <- httr::POST(url, headers, body=body)
			httr::stop_for_status(response)

			json <- httr::content(response)
			ds <- json$dataSet$id

			return (ds)
		},
		schema_definition=function (data) {
			schema <- .self$schema_data(data)
			schema_def <- NULL
			schema_def$columns <- list()
			for (i in 1:length(schema$name)) {
				schema_def$columns[[i]] <- list()
				schema_def$columns[[i]]$name <- schema$name[i]
				schema_def$columns[[i]]$type <- schema$type[i]
			}
			return(schema_def)
		},
		schema_data=function(data) {
			schema <- list()
			if(!is.null(data)) {
				for (i in 1:ncol(data)) {
					t.name <- names(data)[i]
					t.type <- .self$typeConversionText(data,i)
					schema$name[length(schema$name)+1] <- t.name
					schema$type[length(schema$type)+1] <- t.type
				}
			}
			return(schema)
		},
		util_ds_meta=function(ds){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,sep='')
			out <- httr::content((httr::GET(my_url,my_headers)))
			return(out)
		},
		schema_domo=function(ds_id){

			response_content <- .self$util_ds_meta(ds_id)
			response_content$schema$objects <- NULL

			columns <- list()
			for( i in 1:length(response_content$schema$columns)){
				columns$name[length(columns$name)+1] <- response_content$schema$columns[[i]]$name
				columns$type[length(columns$type)+1] <- response_content$schema$columns[[i]]$type
			}
			return(columns)
		},
		typeConversionText=function(data, colindex) {
			result <- 'STRING' #default column type
			date_time <- .self$convertDomoDateTime(data[,colindex])
			if(!is.na(date_time[1])){
				type <- class(date_time)[1]
				if(type == 'Date') result <- 'DATE'
				if(type == 'POSIXct') result <- 'DATETIME'
				if(type == 'POSIXlt') result <- 'DATETIME'
			}else{
				type <- class(data[,colindex])[1]
				if(type == 'character') result <- 'STRING'
				if(type == 'numeric') result <- 'DOUBLE'
				if(type == 'integer') result <- 'LONG'
				if(type == 'Date') result <- 'DATE'
				if(type == 'POSIXct') result <- 'DATETIME'
				if(type == 'factor') result <- 'STRING'
				if(type == 'ts') result <- 'DOUBLE'
			}
			return(result)
		},
		convertDomoDateTime=function(v) {

			date_time <- tryCatch({ as.POSIXct(strptime(v,"%Y-%m-%dT%H:%M:%S")) }, error = function(err) { NA })
			if (is.na(date_time[1]))
				date_time <- tryCatch({ as.POSIXct(strptime(v,"%Y-%m-%d %H:%M:%S")) }, error = function(err) { NA })
			if (is.na(date_time[1]))
				date_time <- tryCatch({ as.Date(v) }, error = function(err) { NA })

			return(date_time)
		},
		get_stream_id=function(ds_id) {

			headers <- httr::add_headers(c("Content-Type"='application/json', Accept='application/json', Authorization=paste('bearer',.self$get_access(), sep=' ')))
			url <- paste('https://',.self$domain,'/v1/streams/search?q=dataSource.id:', ds_id, sep='')

			response <- httr::GET(url, headers)
			httr::stop_for_status(response)

			json <- httr::content(response)

			if(length(json) < 1){
				stop(paste("Unable to find stream for", ds_id))
			}
			return (json[[1]]$id)
		},
		stream_upload=function(ds_id, up_ds){
			df_up <- as.data.frame(up_ds)

			domoSchema <- rjson::toJSON(list(columns=.self$schema_domo(ds_id)))
			dataSchema <- rjson::toJSON(list(columns=.self$schema_data(df_up)))
			
			stream_id <- .self$get_stream_id(ds_id)

			if(!(identical(domoSchema,dataSchema))){
				dataframe_schema <- .self$schema_definition(df_up)
				json <- list(schema=list(columns=dataframe_schema$columns))
				body <- rjson::toJSON(json)
				.self$update_dataset(ds_id, body)
				warning('Schema changed')
			}

			exec_id <- .self$start_execution(stream_id)

			total_rows <- nrow(df_up)

			CHUNKSZ <- .self$estimate_rows(df_up)
			# cat(CHUNKSZ,fill=TRUE)
			start <- 1
			end <- total_rows
			part <- 1
			repeat {
				if (total_rows - end > CHUNKSZ) {
					end <- start + CHUNKSZ
				} else {
					end <- total_rows
				}
				data_frag <- df_up[start:end,]
				.self$uploadPartStr(stream_id, exec_id, part, data_frag)
				part <- part + 1
				start <- end + 1
				if (start >= total_rows){
					break
				}
			}

			result <- .self$commitStream(stream_id, exec_id)
		},
		uploadPartStr=function (stream_id, exec_id, part, data) {
			FNAME <- tempfile(pattern="domo", fileext=".gz")

			headers <- httr::add_headers(c('Content-Type'='text/csv', 'Content-Encoding'='gzip', Accept='application/json', Authorization=paste('bearer',.self$get_access(), sep=' ')))
			url <- paste('https://',.self$domain,"/v1/streams/", stream_id, "/executions/", exec_id, "/part/", part, sep='')

			z <- gzfile(FNAME, "wb")

			readr::write_csv(as.data.frame(data),file=z,col_names=FALSE,na='\\N')
			close(z)

			size <- file.info(FNAME)$size
			b <- readBin(f <- file(FNAME, "rb"), "raw", n=size)
			close(f)

			response <- httr::PUT(url, headers, body=b)
			unlink(FNAME)
			result <- httr::content(response)
			stopifnot(result$status == 200)
		},
		commitStream=function(stream_id, exec_id) {
			headers <- httr::add_headers(c(Accept='application/json', Authorization=paste('bearer',.self$get_access(), sep=' ')))
			url <- paste('https://',.self$domain, "/v1/streams/", stream_id, "/executions/", exec_id, "/commit", sep='')
			response <- httr::PUT(url, headers)
			return(httr::content(response))
		},
		start_execution=function(stream_id) {
			headers <- httr::add_headers(c("Content-Type"='application/json', Accept='application/json', Authorization=paste('bearer',.self$get_access(), sep=' ')))
			url <- paste('https://',.self$domain,"/v1/streams/", stream_id, "/executions", sep='')
			response <- httr::POST(url, headers)
			x <- httr::content(response)
			return(x$id)
		},
		estimate_rows=function (data, kbytes = 10000) {
			sz <- as.numeric(pryr::object_size(data))
			targetSize <- kbytes * 3 # compression factor
			if (sz / 1000 > targetSize)
				return(floor(nrow(data)*(targetSize) / (sz/1000)))
			return(nrow(data))
		},
		update_dataset=function(ds_id, body){
			headers <- httr::add_headers(c("Content-Type"='application/json', Accept='application/json', Authorization=paste('bearer',.self$get_access(), sep=' ')))
			url <- paste('https://',.self$domain,'/v1/datasets/', ds_id, sep='')
			response <- httr::PUT(url, headers, body=body)
			httr::stop_for_status(response)
		}
	)
)

#' Reference class containing functionality to interact with Domo.
#' 
#' Inherits fields from DomoUtilities
#' 
#' All methods documented separately via standard documentation methods.
Domo <- setRefClass("Domo",contains='DomoUtilities',
	methods=list(
		ds_get=function(ds,r_friendly_names=NULL,...){
			"Download data from Domo into a dataframe (tibble)."
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,'/data',sep='')
			out <- httr::content((httr::GET(my_url,my_headers,query=list(includeHeader='true',fileName='bogus.csv'))),na=c('\\N'),...)
			
			# This allows the user to set r_friendly_names = TRUE by default
			rfn <- FALSE
			if( !is.null(r_friendly_names) ){
				rfn <- r_friendly_names
			}
			if( Sys.getenv('RDOMO_RFN') != '' & is.null(r_friendly_names) ){
				rfn <- Sys.getenv('RDOMO_RFN')
			}

			if( rfn ){
				#need to check for duplicate names
				name_check <- data.frame(original=names(out),new=tolower(make.names(names(out))),stringsAsFactors=FALSE)
				dup_check <- dplyr::mutate(dplyr::group_by(name_check,new),rn=dplyr::row_number(),n=dplyr::n())
				new_name <- dplyr::mutate(dup_check,new_name=dplyr::if_else(rn > 1,paste(new,rn,sep='_'),new))

				if( max(dup_check$n) > 1 ){
					warning(paste0('Making friendly R names modified ',sum(new_name$rn > 1),' column names. Beware.'))
				}

				names(out) <- new_name$new_name
			}

			if( sum(class(out) == 'list') > 0 ){
				stop('Error: ',out$error,' -- ',out$error_description)
			}

			return(out)
		},
		ds_create=function(df_up,name,description='',update_method='REPLACE',key_column_names=NA){
			"Create a new data set."
			#creates a stream
			ds <- .self$stream_create(df_up, name, description, update_method,key_column_names)

			#upload
			.self$stream_upload(ds, df_up)
			return(ds)
		},
		ds_update=function(ds_id, df_up){
			"Update an existing data set."
			.self$stream_upload(ds_id, df_up)
		},
		ds_meta=function(...){
			"Get all meta data related to a data set."
			.self$util_ds_meta(...)
		},
		ds_list=function(limit=0,offset=0,df_output=TRUE){
			"List all data sets"
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))

			out <- -1

			if( limit < 1 ){
				n_ret <- 1
				my_batches <- list()
				i <- 1
				batch <- 50 #limit of what the API will return
				while( n_ret > 0 ){
					my_batches[[i]] <- httr::content(httr::GET(paste('https://',.self$domain,'/v1/datasets',sep=''),my_headers,query=list(sort='name',limit=batch,offset=((i-1)*batch))))
					n_ret <- ifelse(length(my_batches[[i]]) < batch,0,1)
					i <- i + 1
				}
				out <- unlist(my_batches,recursive=FALSE)
			}else{
				out <- httr::content(httr::GET(paste('https://',.self$domain,'/v1/datasets',sep=''),my_headers,query=list(sort='name',limit=limit,offset=offset)))
			}

			if( df_output ){
				to_convert <- tibble::tibble(info=out)
				out <- tidyr::unnest_wider(to_convert,info)
			}

			return(out)

		},
		ds_delete=function(ds,prompt_before_delete=TRUE){
			"Delete a data set."
			del_data <- 'Y'
			if( prompt_before_delete ){
				del_data <- invisible(readline(prompt="Permanently delete this data set? This is destructive and cannot be reversed. (Y/n)"))
				cat(del_data,fill=TRUE)
			}
			out_status <- 'Data set not deleted.'
			if( del_data == 'Y' ){
				my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
				my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,sep='')
				out <- (httr::DELETE(my_url,my_headers))
				out_status <- ifelse(out$status_code == 204,'Successfully deleted a dataset','Some Failure')
			}
			return(out_status)
		},
		ds_query=function(ds,query,return_data=TRUE){
			"Evaluate a query against a data set."
			set_type <- function(x,type){
				out <- x
				if( type == 'DOUBLE' ){
					out <- as.numeric(x)
				}
				if( type == 'DATE' ){
					out <- as.Date(x,format='%Y-%m-%d')
				}
				if( type == 'DATETIME' ){
					out <- as.POSIXct(x,format='%Y-%m-%dT%H:%M:%S')
				}
				if( type == 'LONG' ){
					out <- as.integer(x)
				}
				return(out)
			}
			interpret_query <- function(x,col_names,metadata){
				add_names <- x
				names(add_names) <- col_names
				all_types <- lapply(metadata,function(y){y$type})
				get_types <- mapply(set_type,add_names,all_types,SIMPLIFY=FALSE)
				# remove_blanks <- add_names[ add_names != '' ]
				out <- tibble::as_tibble(get_types)
				return(out)
			}
			my_headers <- httr::add_headers(c('Content-Type'='application/json','Accept'='application/json',Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/query/execute/',ds,sep='')
			query_body <- list(
				sql=query
			)
			out <- httr::content((httr::POST(my_url,my_headers,body=rjson::toJSON(query_body))))
			out_out <- out
			if( return_data ){
				out_out <- dplyr::bind_rows(lapply(out$rows,interpret_query,col_names=out$columns,metadata=out$metadata))
			}
			return(out_out)
		},
		ds_rename=function(ds,new_name,new_description=''){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,sep='')
			update <- list(name=new_name)
			if( new_description != '' ){
				update$description <- new_description
			}
			out <- (httr::PUT(my_url,my_headers,body=rjson::toJSON(update)))
			return(out)
		},
		stream_search=function(ds){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/streams/search')
			out <- unlist(httr::content((httr::GET(my_url,my_headers,query=list(q=paste0('dataSource.id:',ds),fields='all')))),recursive=FALSE)
			return(out)
		},
		stream_get=function(stream_id){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/streams/',stream_id)
			out <- httr::content(httr::GET(my_url,my_headers))
			return(out)
		},
		stream_abort=function(stream_id,execution_id){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/streams/',stream_id,'/executions/',execution_id,'/abort')
			out <- httr::content(httr::PUT(my_url,my_headers))
			return(out)
		},
		groups_add_users=function(group_id,users){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			local_user_list <- unlist(users)
			add_uu <- function(uu,grp_id){ httr::content(httr::PUT(paste('https://',.self$domain,'/v1/groups/',grp_id,'/users/',uu,sep=''),my_headers)) }
			out <- sapply(users,add_uu,grp_id=group_id)
			return(out)
		},
		groups_create=function(group_name,users=-1,active='true'){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			local_user_list <- unlist(users)
			my_body <- list(name=group_name,active=active)
			out <- httr::content(httr::POST(paste('https://',.self$domain,'/v1/groups/',sep=''),body=rjson::toJSON(my_body),my_headers))
			if( min(local_user_list) > 0 ){
				tmp <- .self$groups_add_users(out$id,local_user_list)
			}
			return(out)
		},
		groups_delete=function(group_id){
			existing_users <- .self$groups_list_users(group_id)
			del_users <- .self$groups_remove_users(group_id,existing_users)
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			out <- (httr::DELETE(paste('https://',.self$domain,'/v1/groups/',group_id,sep=''),my_headers))
			return(out)
		},
		groups_get=function(group_id){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			out <- httr::content(httr::GET(paste('https://',.self$domain,'/v1/groups/',group_id,sep=''),my_headers))
			return(out)
		},
		groups_list=function(df_output=TRUE){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			n_ret <- 1
			my_batches <- list()
			i <- 1
			batch <- 500
			while( n_ret > 0 ){
				my_batches[[i]] <- httr::content(httr::GET(paste('https://',.self$domain,'/v1/groups',sep=''),my_headers,query=list(limit=batch,offset=((i-1)*batch))))
				n_ret <- ifelse(length(my_batches[[i]]) < batch,0,1)
				i <- i + 1
			}
			out <- unlist(my_batches,recursive=FALSE)
			if( df_output ){
				out <- dplyr::bind_rows(lapply(out,as.data.frame))
			}
			return(out)
		},
		groups_list_users=function(group_id){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			n_ret <- 1
			my_batches <- list()
			i <- 1
			batch <- 500
			while( n_ret > 0 ){
				my_batches[[i]] <- httr::content(httr::GET(paste0('https://',.self$domain,'/v1/groups/',group_id,'/users'),my_headers,query=list(limit=batch,offset=((i-1)*batch))))
				n_ret <- ifelse(length(my_batches[[i]]) < batch,0,1)
				i <- i + 1
			}
			out <- unlist(my_batches,recursive=TRUE)
			return(out)
		},
		groups_remove_users=function(group_id,users){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			local_user_list <- unlist(users)
			remove_uu <- function(uu,grp_id){ httr::content(httr::DELETE(paste('https://',.self$domain,'/v1/groups/',grp_id,'/users/',uu,sep=''),my_headers)) }
			out <- sapply(users,remove_uu,grp_id=group_id)
			return(out)
		},
		page_create=function(page_def){
			my_headers <- httr::add_headers(c('Content-Type'='application/json',Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/pages/')
			out <- httr::content(httr::POST(my_url,my_headers,body=rjson::toJSON(page_def)))
			return(out)
		},
		page_get=function(page_id){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/pages/',page_id)
			out <- httr::content(httr::GET(my_url,my_headers))
			return(out)
		},
		page_get_collections=function(page_id){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/pages/',page_id,'/collections')
			out <- httr::content(httr::GET(my_url,my_headers))
			return(out)
		},
		page_list=function(limit=-1,offset=0){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/pages/')
			if( limit > 0 ){
				out <- httr::content(httr::GET(my_url,my_headers,query=list(limit=limit,offset=offset)))
			}else{
				tt <- httr::content(httr::GET(my_url,my_headers,query=list(limit=50,offset=0)))
				out <- tt
				off <- 50
				while( length(tt) > 0 ){
					tt <- httr::content(httr::GET(my_url,my_headers,query=list(limit=50,offset=off)))
					out <- c(out,tt)
					off <- off + 50
				}
			}
			return(out)
		},
		page_update=function(page_id,page_def){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/pages/',page_id)
			out <- httr::content(httr::PUT(my_url,my_headers,body=rjson::toJSON(page_def)))
			return(out)
		},
		page_delete=function(page_id){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/pages/',page_id)
			out <- httr::content(httr::DELETE(my_url,my_headers))
			return(out)
		},
		#### PDP Related Functions ####
		pdp_create=function(ds,policy_def){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,'/policies',sep='')
			out <- httr::content(httr::POST(my_url,my_headers,body=rjson::toJSON(policy_def)))
			return(out)
		},
		pdp_delete=function(ds,policy){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,'/policies/',policy,sep='')
			out <- httr::content(httr::DELETE(my_url,my_headers))
			return(out)
		},
		pdp_enable=function(ds,new_state){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,sep='')
			my_meta <- .self$util_ds_meta(ds)
			my_body <- list(name=my_meta$name,pdpEnabled=new_state)
			out <- httr::content(httr::PUT(my_url,my_headers,body=rjson::toJSON(my_body)))
			return(out)
		},
		pdp_list=function(ds){
			my_headers <- httr::add_headers(c(Accept="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,'/policies',sep='')
			out <- httr::content(httr::GET(my_url,my_headers))
			return(out)
		},
		pdp_update=function(ds,policy,policy_def){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste('https://',.self$domain,'/v1/datasets/',ds,'/policies/',policy,sep='')
			out <- httr::content(httr::PUT(my_url,my_headers,body=rjson::toJSON(policy_def)))
			return(out)
		},
		#### User Functions ####
		users_add=function(x_name,x_email,x_role,x_sendInvite=FALSE){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			url <- paste0('https://',.self$domain,'/v1/users')
			my_body <- list(
				email=x_email,
				name=x_name,
				role=x_role
			)
			invite_send <- 'false'
			if( x_sendInvite ){
				invite_send <- 'true'
			}
			rr <- httr::POST(url,my_headers,body=rjson::toJSON(my_body),query=list('sendInvite'=invite_send))
			out <- .self$domo_content(rr,success_code=201)
			return(out)
		},
		users_get=function(user_id){
			my_headers <- httr::add_headers(c(Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/users/',user_id)
			out <- httr::content(httr::GET(my_url,my_headers))
			return(out)
		},
		users_list=function(df_output=TRUE,batch_size=500){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			n_ret <- 1
			my_batches <- list()
			i <- 1
			batch <- batch_size
			while( n_ret > 0 ){
				my_batches[[i]] <- tryCatch({
					httr::content(httr::GET(paste('https://',.self$domain,'/v1/users',sep=''),my_headers,query=list(limit=batch,offset=((i-1)*batch))))
				},error=function(e){
					message(e)
					return(NA)
				})
				n_ret <- ifelse(length(my_batches[[i]]) < batch,0,1)
				i <- i + 1
			}
			pre_out <- unlist(my_batches,recursive=FALSE)

			out <- lapply(pre_out,function(x){
				x$createdAt <- as.POSIXct(x$createdAt,'UTC','%Y-%m-%dT%H:%M:%SZ')
				x$updatedAt <- as.POSIXct(x$updatedAt,'UTC','%Y-%m-%dT%H:%M:%OSZ')
				return(x)
			})
			
			if( df_output ){
				out <- dplyr::bind_rows(lapply(out,tibble::as_tibble))
			}
			
			return(out)
		},
		users_update=function(user_id,user_def){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/users/',user_id)
			response <- httr::PUT(my_url,my_headers,body=rjson::toJSON(user_def))
			out <- .self$domo_content(response)
			return(out)
		},
		users_delete=function(user_id){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/users/',user_id)
			rr <- httr::DELETE(my_url,my_headers)
			out <- .self$domo_content(rr,success_code=204)
			return(out)
		},
		
		#### Accounts API ####
		account_list=function(){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/accounts')
			
			n_ret <- 1
			my_accounts <- list()
			i <- 0
			batch <- 50
			while( n_ret > 0 ){
				rr <- httr::GET(my_url,my_headers,query=list(limit=batch,offset=i*batch))
				tt_out <- .self$domo_content(rr)
				my_accounts <- c(my_accounts,tt_out)
				i <- i + 1
				n_ret <- ifelse(length(tt_out) < batch,0,1)
			}
			
			out <- my_accounts
			
			return(out)
		},
		account_get=function(id){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/accounts/',id)
			rr <- httr::GET(my_url,my_headers)
			out <- .self$domo_content(rr)
			return(out)
		},
		account_types=function(){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/account-types/')
			rr <- httr::GET(my_url,my_headers)
			out <- .self$domo_content(rr)
			return(out)
		},
		account_get_type=function(account_type_id){
			my_headers <- httr::add_headers(c(Accept="application/json","Content-Type"="application/json",Authorization=paste('bearer',.self$get_access(),sep=' ')))
			my_url <- paste0('https://',.self$domain,'/v1/account-types/',account_type_id)
			rr <- httr::GET(my_url,my_headers)
			out <- .self$domo_content(rr)
			return(out)
		}
	)
)
