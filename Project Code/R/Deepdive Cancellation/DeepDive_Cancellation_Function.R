### ---------------------------------------------------------------
### --------------------- DeepDive Cancellation -------------------
### ---------------------------------------------------------------

# For the script to run properly, leave all the files in the Deepdive Cancellation folder
# and adjust your working directory below to "/yourFolderLocation/Deepdive Cancellation"


# Goal is to let the script run each month automatically to produce an html markdown output
# "2018/08 - Cancellation & Upsell Report"


# ===================================================
# SETUP
# ===================================================


setwd("C:/Users/Homebell BI/Documents/R/Deepdive Cancellation")    # working directory
rm(list=setdiff(ls(), "plots"))                                    # reset environment
Sys.setenv(LANG = "en")                                            # set language to english
while (!is.null(dev.list()))  dev.off()                            # reset graphics
par(mar=c(2,2,2,2))                                                # prevents problems with plot margins
options(scipen=999)                                                # disables scientific number displaying like 14+e^-20
options(repos=c(CRAN = "https://cloud.r-project.org/"))            # set download mirror
options(deparse.cutoff = 500)                                      # get "...[Truncated]" Output after 500 characters
options(OutDec= ".")                                               # set "." as the decimal separator (otherwise decimal numbers received from redshift get raped)


# Load (or install) R-Packages
packages <- c("tidyverse", "data.table","ggplot2", "ggrepel", "RPostgres", "rjson","devtools",
              "DT","mlr","caret","FSelector","rpart","doParallel","rpart.plot","parallelMap")
for(i in packages) {
  if(!require(i,character.only = TRUE)) install.packages(i)
  library(i,character.only = TRUE)
}
rm(packages, i)

# Load (or install) Homebell-Redshift Package
if(!require(homebellPackage)) install("homebellPackage")
library(homebellPackage)



# ===================================================
# GET DATA
# ===================================================


file <- paste(getwd(),"/devEnvs.json",sep = "")
rs_conn <- create_rs_connection(file)


# DATASET 1: Opportunity Data
query <- "WITH prep AS
(SELECT sr.opportunity_id,
MIN(sr.offer_closed_did) AS first_offer,
MAX(sr.offer_closed_did) AS last_offer,
COUNT(DISTINCT CASE
WHEN ov.bin_upsell THEN sr.offer_id
END) AS n_upsells,
SUM(sr.final_margin)  as margin,
SUM(sr.nmv) AS nmv
FROM facts.sales_revenue sr
LEFT JOIN dims.opportunities op ON op.opportunity_id = sr.opportunity_id
LEFT JOIN dims.offers ov ON ov.offer_id = sr.offer_id
WHERE op.bin_valid_opportunity
AND (op.bin_cancelled
OR op.bin_completed)
GROUP BY 1
ORDER BY 1),

prep2 AS
(SELECT sr.opportunity_id,
sr.sales_agent_id,
sr.main_vertical,
ROW_NUMBER() OVER (PARTITION BY sr.opportunity_id
ORDER BY sr.offer_prepared_did)
FROM facts.sales_revenue sr),

prep3 AS
(SELECT partner_id,
sf_partner_id,
status,
ROW_NUMBER() OVER (PARTITION BY partner_id
ORDER BY sf_partner_id) AS rn
FROM dims.partners),

prep4 AS
(SELECT opportunity_id,
SUM(i.net_invoiced_amount) AS net_inv_amount,
SUM(i.partner_costs) AS partner_cost,
SUM(i.additional_costs) AS additional_cost,
SUM(i.csd) AS csd,
SUM(i.epd) AS epd
FROM facts.invoices i
GROUP BY 1)

SELECT DISTINCT p.opportunity_id,
p.n_upsells,
p.margin,
p.nmv,
op.invoiced_amount,
TO_CHAR('1970-01-01'::date + p.first_offer,'YYYY-MM-DD') AS first_offer,
TO_CHAR('1970-01-01'::date + p.last_offer,'YYYY-MM-DD') AS last_offer,
TO_CHAR('1970-01-01'::date + op.wish_start_did,'YYYY-MM-DD') AS wish_start,
TO_CHAR('1970-01-01'::date + op.job_prepared_did,'YYYY-MM-DD') AS booked,
TO_CHAR('1970-01-01'::date + op.cancellation_did,'YYYY-MM-DD') AS cancelled,
TO_CHAR('1970-01-01'::date + op.first_rematched_did,'YYYY-MM-DD') AS first_rematch,
TO_CHAR('1970-01-01'::date + op.job_completed_did,'YYYY-MM-DD') AS completed,
TO_CHAR(op.downpayment_invoice_sent_at, 'YYYY-MM-DD') AS downp_inv,
op.sf_partner_id AS partner_id,
op.bin_upsold,
op.bin_prepared,
op.bin_rematched,
op.bin_completed,
op.bin_cancelled,
op.bin_won_opp,
op.bin_invoiced,
op.is_image_uploaded,
CASE WHEN op.cancellation_reason = 'No partner found' THEN 1 ELSE 0 END AS no_partner,
CASE WHEN op.cancellation_reason = 'Problems caused by partner' THEN 1 ELSE 0 END AS partner_problem,
a.bin_active,
zm_c.is_urban,
a.name AS first_sales_agent,
cs.name AS cs_agent,
op.zip_id,
zm_c.city_bucket,
zm_c.federal_state AS state,
zm_c.country AS country,
zm_c.population_bucket_radius,
p3.status AS partner_status,
p2.main_vertical,
p4.net_inv_amount,
p4.partner_cost,
p4.additional_cost,
p4.csd,
p4.epd

FROM prep p
LEFT JOIN prep2 p2 ON p2.opportunity_id = p.opportunity_id
AND p2.row_number=1
LEFT JOIN dims.opportunities op ON p.opportunity_id = op.opportunity_id
LEFT JOIN dims.zipcode_mapping zm_c ON zm_c.zip_id = op.zip_id
LEFT JOIN dims.agents a ON a.agent_id = p2.sales_agent_id
LEFT JOIN dims.agents cs ON cs.agent_id = op.ops_agent_id
LEFT JOIN prep3 p3 ON p3.partner_id = op.partner_id AND p3.rn = 1
LEFT JOIN prep4 p4 ON p4.opportunity_id = p.opportunity_id
WHERE p.first_offer >= 17167"

ops <- data.table(df_from_query(rs_conn, query))
setkey(ops,opportunity_id)


# DATASET 2: Individual Vertical Data
query <- "SELECT sr.opportunity_id,
v.vertical,
SUM(sr.opportunity_part_ratio) AS op_part
FROM facts.sales_revenue sr
LEFT JOIN dims.vertical_type v ON v.vertical_type_id = sr.vertical_type_id
LEFT JOIN dims.dates d ON d.date_id = sr.offer_closed_did
LEFT JOIN dims.opportunities op ON op.opportunity_id = sr.opportunity_id
WHERE sr.opportunity_part_ratio IS NOT NULL
AND op.bin_valid_opportunity
AND d.date >= '2017-01-01'
GROUP BY 1,2
ORDER BY 1"

verticals <- data.table(df_from_query(rs_conn, query))
setkey(verticals,opportunity_id,vertical)


# DATASET 3: Aggregate Vertical Data
query <- "WITH prep AS
(SELECT sr.opportunity_id,
CASE
WHEN SUM(opportunity_part_ratio) > 0.05 THEN v.vertical
END AS vertical,
CASE
WHEN SUM(opportunity_part_ratio) > 0.05 THEN 1
END AS count
FROM facts.sales_revenue sr
LEFT JOIN dims.vertical_type v ON v.vertical_type_id = sr.vertical_type_id
GROUP BY 1,
v.vertical)
SELECT p.opportunity_id,
LISTAGG(p.vertical, ' | ') WITHIN
GROUP (
ORDER BY p.vertical) AS verticals,
SUM(count) AS vertical_count
FROM prep p
WHERE p.vertical IS NOT NULL
GROUP BY 1"


vertical_agg <- data.table(df_from_query(rs_conn, query))
setkey(vertical_agg,opportunity_id)


close_rs_connection(rs_conn)
rm(file,query,rs_conn)


# Get Factor-Levels for index vector
idx_vert <- unique(verticals[,vertical])


# Format wide: compress vertical information into one row per opportunity
verticals <- dcast(verticals, opportunity_id ~ vertical, value.var = "op_part")


# Join vertical info with first dataset
full <- merge(ops,vertical_agg,all.x=TRUE)
full <- merge(full,verticals,all.x=TRUE)

setkey(full, opportunity_id)


rm(ops,verticals,vertical_agg)



# ===================================================
# Clean-Up
# ===================================================


# General

# Data-types
fields <- colnames(full[,6:13]) # Date Fields
full[,fields] <- full[ , lapply(X = .SD, FUN = as.Date), .SDcols = fields]

fields <- colnames(full[,c(1,14,27:35,41)]) # Factor Fields
full[,fields] <- full[ , lapply(X = .SD, FUN = as.factor), .SDcols = fields]

fields <- colnames(full[,15:26]) # Logical Fields
full[,fields] <- full[ , lapply(X = .SD, FUN = as.logical), .SDcols = fields]



# Vertical Ratios from NA to zero
for (i in idx_vert) full[is.na(get(i)), (i):=0]



rm(fields,i)




# Checks:

full[bin_completed==0&bin_cancelled==0,.N] # We're looking only @ opps that have been cancelled or completed

full[bin_completed==0&!is.na(completed),.N] # Number of non-completed with completion date
full[bin_completed==1&is.na(completed),.N] # Number of completed without completion date

full[!is.na(cancelled)&!is.na(completed),.N] # Number of completed & cancelled
full[bin_completed==0&!is.na(completed),.(opportunity_id,bin_completed,bin_cancelled,cancelled,completed)] # turns out, the opps in check 1&3 are the same (completed & cancelled)

# >> Filter for bin_completed, when searching for non_cancelled completed
# >> Filter for completed date, when searching for cancellation after completion

full[bin_cancelled==0&!is.na(cancelled),.N] # Number of non-cancelled with cancellation date
full[bin_cancelled==1&is.na(cancelled),.N] # Number of cancelled without cancellation date

full[bin_rematched==0&!is.na(first_rematch),.N] # Number of non-rematched with rematch date
full[bin_rematched==1&is.na(first_rematch),.N] # Number of rematched without rematch date

full[bin_prepared==0&!is.na(booked),.N] # Number of non-prepared with prepared date
full[bin_prepared==1&is.na(booked),.N] # Number of prepared without prepared date

full[bin_prepared==1 & is.na(partner_id),.N] # Number of prepared without partner information
full[bin_prepared==0 & !is.na(partner_id),.N] # Number of unprepared with partner information
full <- full[ ( bin_prepared==1 & !is.na(partner_id) ) | ( bin_prepared==0 & is.na(partner_id) )] # Kick above


# Check for missing information about first closing sales-agent
full[is.na(first_sales_agent),.N]


# Check for missing information about margin / nmv
full[is.na(margin)|is.na(nmv)|nmv==0,.N]


# Check for sum of vertical ratios per opportunity != 1 (100%)
full[,topr := rowSums(.SD), .SDcols = idx_vert]
full[topr<0.98|topr>1.02,.N] # Number of opportunities with sum(vertical ratios) != 100%
full$topr <- NULL



# Check for opportunity-part-ratios assigned to an unknown vertical
# (no occurences since change of data to opportunity level)


# Check for closing dates still @ end of quarter (only completed & cancelled ops: should have proper dates assigned)
full[last_offer>"2018-08-15"&bin_cancelled==0,.N]
full[last_offer>"2018-08-15"&bin_cancelled==1,.N]

full[last_offer>"2018-08-15"&bin_cancelled==1,last_offer:=cancelled] # set to cancellation date



# Kick USA, AT & other country obs other than de / nl

full[substr(zip_id,1,2)!="nl"&substr(zip_id,1,2)!="de",.N]
full <- data.table(full %>% filter(substr(zip_id,1,2) %in% c("nl", "de")))


# zip-id related checks
full[is.na(city_bucket),.N]
full[is.na(state),.N]
full[is.na(country),.N]
full[is.na(country),country:=substr(zip_id,1,2)] # fix obs without country info

full[is.na(population_bucket_radius),.N]


# upsell-check
full[bin_upsold==1&n_upsells==0,.N] # Number of initiated , not yet closed (lost or won) upsells? shouldnt be present here (only cancelled & completed) -> mismatch salesrevenue vs dims.opps
full[bin_upsold==0&n_upsells>0,.N]

full[bin_upsold==1&bin_won_opp==0,.N] # Close/Lost Upsells (since now also marked as cancelled)
full[bin_upsold==1&bin_won_opp==1&bin_cancelled==1,.N] # Close/Won Upsells, that got then cancelled regularly
full[bin_upsold==1&bin_won_opp==1&bin_completed==1,.N] # Close/Won Upsells, that got completed

full[bin_upsold==1,.N]-full[bin_upsold==1&bin_won_opp==1,.N]==full[bin_upsold==1&bin_won_opp==0&bin_cancelled,.N] # Total upsells minus closed_won defined upsells = closed_lost defined upsells? (check whether chosen booleans are valid identifiers)


# checks necessary after letting non-won opps into the data-set (to account for upsold-lost)
full[bin_won_opp==0&bin_completed==1,.N] # opportunity lost but completed -> Kick
full <- full[!(bin_won_opp==0&bin_completed==1)]

full[bin_won_opp==0&bin_cancelled==1&bin_upsold==0,.N] # closed lost + cancelled: only valid for unsuccessfull upsells ... these ones are not upsells however -> kick
full <- full[!(bin_won_opp==0&bin_cancelled==1&bin_upsold==0)]


# Check for duplicates
full[,.N,by=opportunity_id][N!=1,.N]

# facts.invoices checks
full[is.na(net_inv_amount)&bin_invoiced==1,.N] # number of opportunities without facts.invoices info but with bin_invoiced flag
full[!is.na(net_inv_amount)&bin_invoiced==0,.N] # number of opportunities without bin_invoiced flag but with invoice info from facts.invoices

full[!is.na(net_inv_amount)&bin_invoiced==0,bin_invoiced:=TRUE] # Fixing the above (setting bin_invoiced flag)


full[(epd>0|csd>0)&bin_completed==1,.N] # number of completed opps with discount
full[(epd>0|csd>0)&bin_cancelled==1,.N] # number of cancelled opps with discount (zero... not usable as cancellation predictor)

# difference between nmv of invoiced opportunities and net invoiced amount
summary(full[bin_invoiced==1,.(nmv,net_inv_amount)])
full[bin_invoiced==1&nmv!=net_inv_amount,.N]/full[bin_invoiced==1,.N] # percentage of invoiced opps where the two aren't matching

full[(no_partner==1|partner_problem==1)&bin_cancelled==0,.N] # number of opportunities with cancellation reason that are not marked as bin_cancelled


# set epd/csd NAs to zero



# ===================================================
# Feature-Engineering
# ===================================================


# make margin a ratio of nmv
full[,margin:=margin/nmv]
full[margin>0.5,.N] # Number of opps with margin > 50%
full[margin<0,.N] # Number of opps with negative margin
# full <- full[margin<0.5&margin>=0] # Kick margin > 50% & negative margins (indicates deviation from standard cases = deviation from standard cancel-reasons. we want to make universally valid statements about cancellation)


# invoiced margin
full[,inv_margin:=(net_inv_amount-partner_cost-additional_cost)/net_inv_amount]


# time-differences as cancellation predictors
full[,close2book:=as.numeric(booked-last_offer,units="days")]
full[,last_close2cancel:=as.numeric(cancelled-last_offer,units="days")]
full[,book2cancel:=as.numeric(cancelled-booked,units="days")]
full[,downp_inv2cancel:=as.numeric(cancelled-downp_inv,units="days")]
full[,sale_period:=as.numeric(last_offer-first_offer,units="days")]
full[,vorlauf:=as.numeric(wish_start-last_offer,units="days")]# Vorlauf


full[bin_prepared==1,min(close2book)]
full[bin_prepared==1&close2book<0,close2book:=0]


cor(x = full[!is.na(downp_inv2cancel)&!is.na(book2cancel),downp_inv2cancel], y = full[!is.na(downp_inv2cancel)&!is.na(book2cancel),book2cancel])
# the downpayment invoice date is almost identical to the booking date.
# high cancellation rates shortly after the booking date or the invoice date
# can therefore not be unambiguously associated with one of the two cancellation reasons
# - insufficient information about downpayment by sales-agent
# - partner advises to cancel contract with homebell

full[downp_inv2cancel!=book2cancel,.N] # Number of occurences where dates are different
full$downp_inv <- NULL                 # Drop downpayment invoice date as predictor variable 
full$downp_inv2cancel <- NULL          # (no added value when having booking date)

# Also Daniel is convinced that we send this invoice way earlier than our timestamp suggests (Ask Basti)

# wish-start-monat
full[,month_ws:=as.factor(format(wish_start, "%m"))]






# ===================================================
# Descriptive Analysis Function
# ===================================================


analyze <- function(subject,
                    filter="",
                    minJobs=0,
                    last365=FALSE,
                    upsells=FALSE,
                    splitCancels=FALSE,
                    partnerStatus=FALSE,
                    book2cancel=FALSE,
                    time2match=TRUE,
                    c_reason=TRUE
) {
  
  
  
  # preparing filter
  if ( nchar(filter)==0 & last365==T ) {
    filter<-"wish_start>=Sys.Date()-400&wish_start<=Sys.Date()-35"
  } else if ( nchar(filter)>0 & last365==T ) {
    filter<- paste(filter,"&wish_start>=Sys.Date()-400&wish_start<=Sys.Date()-35")
  }
  
  
  # Subsetting & Filtering
  data <- full[eval(parse(text = paste(filter))), # insert user-entered filters
               .(opportunity_id, # for length
                 eval(parse(text = paste(subject))),
                 bin_won_opp, # for filtering out lost upsells
                 bin_prepared,
                 bin_completed,
                 bin_cancelled,
                 bin_invoiced,
                 bin_upsold,
                 sale_period,
                 book2cancel,
                 last_close2cancel,
                 close2book,
                 partner_status,
                 margin,
                 nmv,
                 inv_margin,
                 net_inv_amount,
                 no_partner,
                 partner_problem
               )]
  
  fields<-c("V2")
  if ( partnerStatus==TRUE ) {
    fields <-c(fields,"partner_status") 
  }
  
  
  # counts
  
  data[,n_jobs:=.N,by=.(V2)] # total number of opportunities associated with the analysis subject
  data[,n_cancelled:=length(opportunity_id[bin_cancelled==1&bin_won_opp==1]),by=.(V2)] # number of cancelled opportunities
  data[,n_completed:=length(opportunity_id[bin_completed==1]),by=.(V2)] # number of completed opportunities
  fields <-c(fields,"n_jobs") 
  
  if ( splitCancels==TRUE ) {
    data[,n_cancel_prebook:=length(opportunity_id[bin_cancelled==1&bin_prepared==0&bin_won_opp==1]),by=.(V2)] # number of prebook cancelled opportunities
    data[,n_cancel_postbook:=length(opportunity_id[bin_cancelled==1&bin_prepared==1&bin_won_opp==1]),by=.(V2)] # number of postbook cancelled opportunities
  }
  
  if ( upsells==TRUE ) {
    data[,n_up:=length(opportunity_id[bin_upsold==1]),by=.(V2)] # number of upsold opportunities
    data[,n_up_w:=length(opportunity_id[bin_upsold==1&bin_won_opp==1]),by=.(V2)] # number of upsold/won opportunities
    
    
    # nmv
    
    data[,up_opps_nmv:=sum(nmv[bin_upsold==1]),by=.(V2)]
    data[,up_w_opps_nmv:=sum(nmv[bin_upsold==1&bin_won_opp==1]),by=.(V2)]
  }
  
  if ( splitCancels==1 ) {
    data[,cancelled_prebook_nmv:=sum(nmv[bin_cancelled==1&bin_prepared==0&bin_won_opp==1]),by=.(V2)]
    data[,cancelled_postbook_nmv:=sum(nmv[bin_cancelled==1&bin_prepared==1&bin_won_opp==1]),by=.(V2)]
  }
  
  data[,cancelled_nmv:=sum(nmv[bin_cancelled==1&bin_won_opp==1]),by=.(V2)]
  data[,completed_nmv:=sum(nmv[bin_completed==1]),by=.(V2)]
  data[,invoiced_nmv:=sum(nmv[bin_invoiced==1]),by=.(V2)]
  data[,invoiced_net_amount:=sum(net_inv_amount[bin_invoiced==1]),by=.(V2)]
  
  
  # ratios by number
  
  if ( splitCancels==1 ) {
    data[,c_rate_prebook_num:=round(n_cancel_prebook/n_jobs,digits = 2)]
    data[,c_rate_postbook_num:=round(n_cancel_postbook/n_jobs,digits = 2)]
    fields <-c(fields,"c_rate_prebook_num","c_rate_prebook_nmv","c_rate_postbook_num","c_rate_postbook_nmv") 
  }
  
  data[,c_rate_total_num:=round(n_cancelled/n_jobs,digits = 2)]
  fields <-c(fields,"c_rate_total_num","c_rate_total_nmv") 
  
  if ( upsells==TRUE ) {
    data[,up_rate_total_num:=round(n_up/n_jobs,digits=2)]
    data[,up_rate_won_num:=round(n_up_w/n_up,digits=2)]
    fields <-c(fields,"up_rate_total_num","up_rate_total_nmv","up_rate_won_num","up_rate_won_nmv") 
  }
  
  if ( c_reason==TRUE ) {
    data[,cr_no_partner_found_num:=round(sum(no_partner)/n_cancelled,digits = 2),by=.(V2)]
    data[,cr_partner_problem_num:=round(sum(partner_problem)/n_cancelled,digits = 2),by=.(V2)]
    fields <- c(fields,"cr_no_partner_found_num","cr_partner_problem_num")
  }
  
  
  # ratios by nmv
  
  if ( splitCancels==1 ) {
    data[,c_rate_prebook_nmv:=round(cancelled_prebook_nmv/sum(nmv),digits = 2),by=.(V2)]
    data[,c_rate_postbook_nmv:=round(cancelled_postbook_nmv/sum(nmv),digits = 2),by=.(V2)]
  }
  
  if ( upsells==TRUE ) {
    data[,up_rate_total_nmv:=round(up_opps_nmv/sum(nmv),digits=2),by=.(V2)]
    data[,up_rate_won_nmv:=round(up_w_opps_nmv/up_opps_nmv,digits=2)]
  }
  
  data[,c_rate_total_nmv:=round(cancelled_nmv/sum(nmv),digits = 2),by=.(V2)]
  data[,med_invoiced_margin_nmv:=round(median(inv_margin[bin_invoiced==1]),digits = 2),by=.(V2)]
  data[,med_invoiced_nmv:=round(median(nmv[bin_invoiced==1]),digits = 2),by=.(V2)]
  data[,med_invoiced_margin:=round(median(inv_margin[bin_invoiced==1]),digits = 2),by=.(V2)]
  data[,med_invoiced_net_amount:=round(median(net_inv_amount[bin_invoiced==1]),digits = 2),by=.(V2)]
  
  fields <-c(fields,"med_invoiced_margin_nmv","med_invoiced_margin","med_invoiced_nmv","med_invoiced_net_amount") 
  
  
  
  # time-diffs
  data[,med_close_to_cancel:=median(last_close2cancel, na.rm = TRUE),by=.(V2)]
  data[,d14_cancels:=round(length(opportunity_id[last_close2cancel<=14&bin_cancelled])/n_cancelled,digits=2),by=.(V2)]
  fields <-c(fields,"med_close_to_cancel","d14_cancels")
  
  if ( book2cancel==TRUE ) {
    data[,med_book_to_cancel:=median(book2cancel, na.rm = TRUE),by=.(V2)]
    fields <- c(fields,"med_book_to_cancel")
  }
  
  if ( upsells==TRUE ) {
    data[,med_sale_period:=median(sale_period, na.rm = TRUE),by=.(V2)]
    fields <-c(fields,"med_sale_period")
  }
  
  if ( time2match==TRUE ) {
    data[,med_close_to_book:=median(close2book, na.rm = TRUE),by=.(V2)]
    data[,mean_close_to_book:=mean(close2book, na.rm = TRUE),by=.(V2)]
    fields <-c(fields,"med_close_to_book","mean_close_to_book")
  }
  
  
  data <- unique(data[n_jobs>minJobs,.SD,.SDcols=fields])[order(rank(V2))]
  
  names(data)[1] <- subject # replacing "V2" column name by current subject
  
  if ( partnerStatus==TRUE ) {
    quantiles <- summary(data[,3:ncol(data)])
  } else {
    quantiles <- summary(data[,2:ncol(data)])
  }
  
  sub<-list()
  sub[["data"]]<-data
  sub[["quantiles"]]<-quantiles
  return(sub) 
  
  
} # end of function



# ===================================================
# Descriptive Analysis
# ===================================================


# By Last Partner

sub_partners <- analyze(subject = "partner_id",
                        filter = "bin_prepared==1&!is.na(partner_id)&bin_won_opp==1",
                        minJobs = 5,
                        last365 = T,
                        upsells = F,
                        splitCancels = F,
                        partnerStatus = T,
                        book2cancel = T,
                        time2match = F,
                        c_reason = T)


# By First Sales-Agent

sub_agents <- analyze(subject = "first_sales_agent",
                      filter = "bin_active==1",
                      minJobs = 10,
                      last365 = F,
                      upsells = F,
                      splitCancels = T,
                      partnerStatus = F,
                      book2cancel = F,
                      time2match = F,
                      c_reason = T)


# By Month (seasonal analysis)

sub_seasonal <- list() 
for (j in c("de","nl","de'|country=='nl")) {
  sub <- list()
  for (i in idx_vert) { 
    sub[[i]] <- analyze(subject = "month_ws",
                        filter = paste("get(i)>0&country=='",j,"'",sep=""),
                        minJobs = 5,
                        last365 = T,
                        upsells = F,
                        splitCancels = T,
                        partnerStatus = F,
                        book2cancel = F,
                        time2match = T,
                        c_reason = T)
  }
  sub_seasonal[[j]]  <- sub
  rm(sub)
}
rm(i,j)




# By City

sub_city <- analyze(subject = "city_bucket",
                    filter = "!is.na(city_bucket)",
                    minJobs = 10,
                    last365 = T,
                    upsells = F,
                    splitCancels = T,
                    partnerStatus = F,
                    book2cancel = F,
                    time2match = T,
                    c_reason = T)



# By Vertical

sub_vertical <- list()
sub<-list()

for (j in c("de","nl","de'|country=='nl")) {
  sub[[j]] <- analyze(subject = "verticals",
                      filter = paste("country=='",j,"'",sep=""),
                      minJobs = 5,
                      last365 = T,
                      upsells = F,
                      splitCancels = T,
                      partnerStatus = F,
                      book2cancel = F,
                      time2match = T,
                      c_reason = T)
}
sub_vertical[["vertical_combo"]]<-sub

for (j in c("de","nl","de'|country=='nl")) {
  sub[[j]] <- analyze(subject = "vertical_count",
                      filter = paste("country=='",j,"'",sep=""),
                      minJobs = 10,
                      last365 = T,
                      upsells = F,
                      splitCancels = T,
                      partnerStatus = F,
                      book2cancel = F,
                      time2match = T,
                      c_reason = T)
}
sub_vertical[["vertical_count"]]<-sub

for (j in c("de","nl","de'|country=='nl")) {
  sub[[j]] <- analyze(subject = "main_vertical",
                      filter = paste("country=='",j,"'",sep=""),
                      minJobs = 10,
                      last365 = T,
                      upsells = F,
                      splitCancels = T,
                      partnerStatus = F,
                      book2cancel = F,
                      time2match = T,
                      c_reason = T)
}
sub_vertical[["main_vertical"]]<-sub
rm(j,sub)



# By Vertical and City

sub_vertical_city <- list()
for (i in idx_vert) { 
  sub_vertical_city[[i]] <- analyze(subject = "city_bucket",
                                    filter = "!is.na(city_bucket)",
                                    minJobs = 10,
                                    last365 = T,
                                    upsells = F,
                                    splitCancels = T,
                                    partnerStatus = F,
                                    book2cancel = F,
                                    time2match = T,
                                    c_reason = T)
}
rm(i,analyze,idx_vert)




# ===================================================
# PREPARING DATA FOR MODELLING
# ===================================================


  # Building Datasets

    # Build c_rates
      full[,partner_c_rate:=length(opportunity_id[bin_cancelled==1])/.N,by=partner_id]
      full[,vertical_combo_c_rate:=length(opportunity_id[bin_cancelled==1])/.N,by=verticals]
      full[,agent_c_rate:=length(opportunity_id[bin_cancelled==1])/.N,by=first_sales_agent]

    sub<-list()
  
    # Prebook Cancellation Set (5 predictors)
      sub[["pre"]] <- full[bin_prepared==0|bin_completed,.(bin_cancelled,sale_period,vorlauf,nmv,vertical_combo_c_rate,agent_c_rate,is_image_uploaded,is_urban)]
  
    # Postbook Cancellation Set (9 predictors)
      sub[["post"]] <- full[bin_prepared==1,.(bin_cancelled,sale_period,vorlauf,close2book,bin_rematched,nmv,margin,vertical_combo_c_rate,partner_c_rate,agent_c_rate,is_image_uploaded,is_urban)]

      
  # Treatment of outliers

      for (i in c("vorlauf","nmv")) {
        qnt <- sub[["pre"]][, quantile(get(i), probs=c(.25, .75), na.rm = T)]
        H <- sub[["pre"]][, 1.5 * IQR(get(i), na.rm = T)]
        sub[["pre"]][get(i) < (qnt[1] - H), .N]
        sub[["pre"]][get(i) > (qnt[2] + H), .N]
        sub[["pre"]][get(i) < (qnt[1] - H), i] <- (qnt[1] - H)
        sub[["pre"]][get(i) > (qnt[2] + H), i] <- (qnt[2] + H)
      }
      
      for (i in c("vorlauf","close2book","nmv","margin")) {
        qnt <- sub[["post"]][, quantile(get(i), probs=c(.25, .75), na.rm = T)]
        H <- sub[["post"]][, 1.5 * IQR(get(i), na.rm = T)]
        sub[["post"]][get(i) < (qnt[1] - H), .N]
        sub[["post"]][get(i) > (qnt[2] + H), .N]
        sub[["post"]][get(i) < (qnt[1] - H), i] <- (qnt[1] - H)
        sub[["post"]][get(i) > (qnt[2] + H), i] <- (qnt[2] + H)
      }
      
      
  # Treatment of missing values
      # only one observation in "pre" -> kick
      sub[["pre"]]<-na.omit(sub[["pre"]])
      sub[["post"]]<-na.omit(sub[["post"]])
      
      
  # Standardization
      fields <- sapply(X = sub[["pre"]], FUN = is.numeric)
      fields <- names(fields[fields==TRUE])
      sub[["pre"]][,fields] <- sub[["pre"]][ , lapply(X = .SD, FUN = scale), .SDcols = fields]
      
      fields <- sapply(X = sub[["post"]], FUN = is.numeric)
      fields <- names(fields[fields==TRUE])
      sub[["post"]][,fields] <- sub[["post"]][ , lapply(X = .SD, FUN = scale), .SDcols = fields]
      
      
  # logical to binary
      fields <- sapply(X = sub[["pre"]], FUN = is.logical)
      fields <- names(fields[fields==TRUE])
      sub[["pre"]][,fields] <- sub[["pre"]][ , lapply(X = .SD, FUN = as.numeric), .SDcols = fields]
      sub[["pre"]][,fields] <- sub[["pre"]][ , lapply(X = .SD, FUN = as.factor), .SDcols = fields]
      
      fields <- sapply(X = sub[["post"]], FUN = is.logical)
      fields <- names(fields[fields==TRUE])
      sub[["post"]][,fields] <- sub[["post"]][ , lapply(X = .SD, FUN = as.numeric), .SDcols = fields]
      sub[["post"]][,fields] <- sub[["post"]][ , lapply(X = .SD, FUN = as.factor), .SDcols = fields]
      

rm(fields,i,H,qnt)



  # convert to subsets to data.frame format
  sub[["pre"]] <- data.frame(sub[["pre"]])
  sub[["post"]] <- data.frame(sub[["post"]])


# -----------------------------------
# PREPARING TASKS FOR LEARNERS
# -----------------------------------
  
  
# Define tasks
  #(excluded sales-period because a longer period had a negative impact on predicted cancellation
  # probability. This is because upsells are almost entirely won in this dataset)
  
task.pre <- makeClassifTask(data = sub[["pre"]][,!colnames(sub[["pre"]])=="sale_period"],
                            target = "bin_cancelled", positive = 1, id="pred_pre")

task.post <- makeClassifTask(data = sub[["post"]][!colnames(sub[["post"]])=="sale_period"],
                            target = "bin_cancelled", positive = 1, id="pred_post")


# Check qualification of predictor variables

fv_pre <- generateFilterValuesData(task = task.pre, method = c("information.gain","chi.squared"))
  fv_pre$data
  plotFilterValues(fv_pre)
  fv_pre <- generateFilterValuesData(task = task.pre, method = "information.gain")
  
fv_post <- generateFilterValuesData(task = task.post, method = c("information.gain","chi.squared"))
  fv_post$data
  plotFilterValues(fv_post)
  fv_post <- generateFilterValuesData(task = task.post, method = "information.gain")

  # excluding the c_rates of vertical_combo / agent / partner does not increase the measured
  # information gain of the other predictors
  
  
# Select the information gain threshold, qualifying a variable to be considered:
TS = 0.000     # setting to zero will leave all variables in the dataset

task.pre.filt <- filterFeatures(task.pre, fval = fv_pre, threshold = TS)
task.post.filt <- filterFeatures(task.post, fval = fv_post, threshold = TS)

rm(TS, fv_pre, fv_post,task.pre,task.post)




# -----------------------------------
# LOGISTIC REGRESSION
# -----------------------------------

# logistic regression
logistic.learner <- makeLearner("classif.logreg",predict.type = "prob")


log_reg <- function(learner = logistic.learner, task = task.pre_ex){
  # average accuracy on 5 fold cross validation - prebook cancellation set
  cv.logistic_pre_ex <- crossval(learner = logistic.learner,
                        task = task.pre_ex,
                        iters = 5,
                        stratify = TRUE,
                        measures = list(mlr::auc,mlr::acc),
                        show.info = F)


  print(cv.logistic_pre_ex$aggr)
  model <- mlr::train(logistic.learner,task)
  print(getLearnerModel(model))
  return(model)
}





# -----------------------------------
# DECISION TREE ANALYSIS
# -----------------------------------
dtree.learner <- makeLearner("classif.rpart", predict.type = "prob")

#getParamSet("classif.rpart")     # <- uncomment to get list of tunable / non-tunable parameters

# parameter tuning settings
set_cv <- makeResampleDesc("CV",iters = 3L, stratify=TRUE) # settings for 3fold crossvalidation

gs <- makeParamSet(
  makeIntegerParam("minsplit",lower = 40, upper = 120),
  makeIntegerParam("minbucket", lower = 5, upper = 40),
  makeNumericParam("cp", lower = 0.0001, upper = 0.1)
  )

gscontrol <- makeTuneControlGrid()


# start parallel processing with all but one core for hyperparametertuning
parallelStartSocket(max(1,detectCores()-2), level = "mlr.tuneParams")

stune_pre <- tuneParams(learner = dtree.learner, 
                    resampling = set_cv, 
                    task = task.pre.filt, 
                    par.set = gs, 
                    control = gscontrol, 
                    measures = list(mlr::auc,mlr::acc))

stune_post <- tuneParams(learner = dtree.learner, 
                    resampling = set_cv, 
                    task = task.post.filt, 
                    par.set = gs, 
                    control = gscontrol, 
                    measures = list(mlr::auc,mlr::acc))
parallelStop()

# best parameters in set range & their accuracy on crossvalidation for prebook cancellation set
stune_pre$x # cp = complexity parameter: the lower the more specific relations are learned (overfitting)
stune_pre$y

# best parameters in set range & their accuracy on crossvalidation for postbook cancellation set
stune_post$x
stune_post$y

#using hyperparameters for modeling
dtree.learner_pre <- setHyperPars(dtree.learner, par.vals = stune_pre$x)
dtree.learner_post <- setHyperPars(dtree.learner, par.vals = stune_post$x)


# Decision Tree Analysis results of Prebook Cancellation Set
rdesc <- makeResampleDesc(method = "CV", iters = max(1,detectCores()-2), stratify = TRUE)
parallelStartSocket(max(1,detectCores()-2), level = "mlr.resample")
resample(learner = dtree.learner_pre,task = task.pre.filt,resampling = rdesc,measures = list(mlr::auc,mlr::acc))
parallelStop()
generateFeatureImportanceData(task = task.pre.filt,learner = dtree.learner_pre)

# Decision Tree Analysis results of Postbook Cancellation Set
rdesc <- makeResampleDesc(method = "CV", iters = max(1,detectCores()-2), stratify = TRUE)
parallelStartSocket(max(1,detectCores()-2), level = "mlr.resample")
resample(learner = dtree.learner_pre,task = task.pre.filt,resampling = rdesc,measures = list(mlr::auc,mlr::acc))
parallelStop()
generateFeatureImportanceData(task = task.post.filt,learner = dtree.learner_post)

