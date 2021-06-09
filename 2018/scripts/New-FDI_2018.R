# Call for data for the Fisheries Dependent Information 2018 --------------
## New-FDI 2018

# Definition of global environment and setup ------------------------------
path="D:\\IRD\\5 - Projets\\3 - ADD\\2 - FDI\\2018"
library(RPostgreSQL)
library(dplyr)
library(rgeos)
library(rgdal)

# Databases connections ---------------------------------------------------
## You need a connection with the IRD's VPN for the host vmot5-proto.ird.fr

t3_con=dbConnect(drv=dbDriver("PostgreSQL"),
                 dbname="t3_prod",
                 host="vmot5-proto.ird.fr",
                 port="5432",
                 user="t3-admin",
                 password="dm8t3")

observe_con=dbConnect(drv=dbDriver("PostgreSQL"), 
                      dbname="observe",
                      host="vmot5-proto.ird.fr",
                      port="5432",
                      user="utilisateur",
                      password="util8obstuna")

balbaya_con=dbConnect(drv=dbDriver("PostgreSQL"),
                      dbname="balbaya",
                      host="aldabra2",
                      port="5432",
                      user="mdepetris",
                      password="iv10balba")

sardara_con=dbConnect(drv=dbDriver("PostgreSQL"),
                      dbname="sardara",
                      host="aldabra2",
                      port="5432",
                      user="invsardara",
                      password="iv9sarda")

# balbaya_con=dbConnect(drv=dbDriver("PostgreSQL"),
#                       dbname="balbaya",
#                       host="vmot5-proto.ird.fr",
#                       port="5432",
#                       user="mdepetris",
#                       password="iv10balba")

#dbDisconnect(t3_con)
#dbDisconnect(observe_con)
#dbDisconnect(balbaya_con)

# ObServe rasing factor ---------------------------------------------------
## Number of activity and weight of catch from obServe
observe_obscatchwt_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "obscatchwt_obServe.sql", fsep="\\")), collapse="\n")
observe_observe_obscatchwt_sqlQryDf=dbGetQuery(observe_con, observe_obscatchwt_sqlQry)

## Number of activity and weight of catch from balbaya
balbaya_catchwtactnum_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "catchwtactnum_balbaya.sql", fsep="\\")), collapse="\n")
balbaya_catchwtactnumDf=dbGetQuery(balbaya_con, balbaya_catchwtactnum_sqlQry)

obsRfDf=balbaya_catchwtactnumDf %>% 
  full_join(observe_observe_obscatchwt_sqlQryDf) %>%
  mutate(rf=ifelse(ocean=='ATL' & metier=="PS_LPF_0_0_0" & year %in% c(2015,2016), 1, act_number/obs_act_number)) %>%
  mutate(rf=ifelse(ocean=='ATL' & metier=="LHP_LPF_0_0_0", 0, rf)) %>%
  select(country, year, metier, ocean, schooltype, rf)

# Table A : Catch data for 2015,2016,2017 ---------------------------------
## Catch data from balbaya
balbaya_tableA_catchDf_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableA_catch_data_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
balbaya_tableA_catchDf=dbGetQuery(balbaya_con, balbaya_tableA_catchDf_sqlQry)

## Unwanted catch form obServ
obServe_tableA_discard_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableA_discard_data_2015_2016_2017_obServe.sql", fsep="\\")), collapse="\n")
obServe_tableA_discardDf=dbGetQuery(observe_con, obServe_tableA_discard_sqlQry)

## Merge
tableA_catch=balbaya_tableA_catchDf %>%
  left_join(obServe_tableA_discardDf)

tableA_catch$unwanted_catch[is.na(tableA_catch$unwanted_catch)]=0
tableA_catch$domain_discards=tableA_catch$domain
tableA_catch$domain_landings=tableA_catch$domain

## Raising of discard number by number of activity on number of observation
obsRfDf$idrf=paste(obsRfDf$year, obsRfDf$metier, obsRfDf$ocean, obsRfDf$schooltype, sep="_")

for (i in 1:dim(tableA_catch)[1]) {
  tableA_catch[i, "idrf"]=paste(tableA_catch[i, "year"],
                                tableA_catch[i, "metier"],
                                tableA_catch[i, "ocean"],
                                unlist(strsplit(tableA_catch[i, "domain"], '_'))[length(unlist(strsplit(tableA_catch[i, "domain"], '_')))],
                                sep="_")
}
rm(i)

tableA_catch=tableA_catch %>%
  left_join(obsRfDf[,c("idrf", "rf")], by="idrf") %>%
  mutate(rf=ifelse(is.na(rf), 1, rf)) %>%
  mutate(unwanted_catch=round(unwanted_catch*rf, digits=3))

## Design and extraction
tableA_catch=tableA_catch %>%
  select(country, year, quarter, vessel_length, fishing_tech, gear_type, mesh_size_range, metier, domain_discards,
         domain_landings, supra_region, sub_region, eez_indicator, geo_indicator, specon_tech, target_assemblage, 
         deep, species, totwghtlandg, totvallandg, unwanted_catch, confidential) %>%
  arrange(country, year, quarter, vessel_length, fishing_tech, gear_type, mesh_size_range, metier, domain_discards,
          domain_landings, supra_region, sub_region, eez_indicator, geo_indicator, specon_tech, target_assemblage, 
          deep, species, totwghtlandg, totvallandg, unwanted_catch, confidential)

names(tableA_catch)=toupper(names(tableA_catch))

write.table(x=tableA_catch, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_A_CATCH.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Table D : Unwanted catch biological data for 2015, 2016 and 2017 --------
observe_tableD_discard_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableD_discard_data_length_2015_2016_2017_obServe.sql", fsep="\\")), collapse="\n")
observe_tableD_discardsDf=dbGetQuery(observe_con, observe_tableD_discard_sqlQry)

observe_tableD_cas_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableD_cas_data_length_2015_2016_2017_obServe.sql", fsep="\\")), collapse="\n")
observe_tableD_casDf=dbGetQuery(observe_con, observe_tableD_cas_sqlQry)

balbaya_tableF_landing_weights_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableF_landing_data_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
balbaya_tableF_landing_weightsDf=dbGetQuery(balbaya_con, balbaya_tableF_landing_weights_sqlQry)

## Merge and design
tableD=observe_tableD_discardsDf %>%
  left_join(obsRfDf) %>%
  left_join(balbaya_tableF_landing_weightsDf) %>%
  mutate(totwghtlandg=ifelse(test=is.na(totwghtlandg), yes=0, no=totwghtlandg)) %>%
  # group_by(country, year, metier, ocean, schooltype, species) %>%
  # mutate(prop=totwghtlandg/sum(totwghtlandg)) %>%
  # ungroup() %>%
  # mutate(newunwanted_catch=prop*unwanted_catch)
  full_join(observe_tableD_casDf) %>%
  mutate(unwanted_catch=round(unwanted_catch*rf, 3),
         no_length_uc=no_length_uc*rf) %>%
  mutate(no_length_uc=ifelse(no_length_uc > 0 & no_length_uc < 0.5, 1, round(no_length_uc))) %>%
  mutate(length_unit="cm") %>%
  select(country, year, domain_discards, species, totwghtlandg, unwanted_catch, no_samples_uc, no_length_measurements_uc, length_unit, min_length, max_length, length, no_length_uc) %>%
  arrange(country, year, domain_discards, species, length)

names(tableD)=toupper(names(tableD))

##Probleme au niveau des tailles, � verifier lors de la prochaine soumission de donn�es
tmp=data.frame()
for (i in 1:dim(tableD)[1]) {
  if (tableD[i, "LENGTH"] >= tableD[i, "MIN_LENGTH"]) {
    if (tableD[i, "LENGTH"] <= tableD[i, "MAX_LENGTH"]) {
      tmp=rbind(tmp, tableD[i,])
    }
  }
}
tableD=tmp
rm(i, tmp)

## Extraction
write.table(x=tableD, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_D_UNWANTED_CATCH_AT_LENGTH.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Table F : landings biological data (length based) for 2015, 2016 and 2017 --------

## Trips sampled
t3_tableF_trips_sampled_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableF_trips_sampled_2015_2016_2017_t3.sql", fsep="\\")), collapse="\n")
t3_tableF_trips_sampledDf=dbGetQuery(t3_con, t3_tableF_trips_sampled_sqlQry)

## Individus measured
balbaya_tableF_ind_measured_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableF_ind_measured_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
balbaya_tableF_ind_measuredDf=dbGetQuery(balbaya_con, balbaya_tableF_ind_measured_sqlQry)

## Min and max length
sardara_tableF_min_max_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableF_min_max_length_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
sardara_tableF_min_maxDf=dbGetQuery(sardara_con, sardara_tableF_min_max_sqlQry)

## CAS
sardara_tableF_cas_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableF_cas_2015_2016_2017_sardara.sql", fsep="\\")), collapse="\n")
sardara_tableF_casDf=dbGetQuery(sardara_con, sardara_tableF_cas_sqlQry)

## Merge A VOIR
tableF=balbaya_tableF_landing_weightsDf %>% 
  full_join(t3_tableF_trips_sampledDf) %>%
  filter(!is.na(act_number)) %>%
  mutate(domain_landings="NK") %>%
  full_join(balbaya_tableF_ind_measuredDf %>% mutate(lengthunit="cm")) %>%
  mutate(no_length_measurements_landg=ifelse(no_length_measurements_landg > 0 & no_length_measurements_landg < 0.5 & no_length_measurements_landg!="NK", 1, round(no_length_measurements_landg))) %>%
  mutate(no_length_measurements_landg=ifelse(is.na(no_length_measurements_landg), "NK", no_length_measurements_landg)) %>%
  full_join(sardara_tableF_min_maxDf) %>%
  mutate(min_length=ifelse(is.na(min_length), "NK", min_length)) %>%
  mutate(max_length=ifelse(is.na(max_length), "NK", max_length)) %>%
  full_join(sardara_tableF_casDf) %>%
  mutate(lengthunit=ifelse(is.na(lengthunit), "NK", lengthunit)) %>%
  mutate(length=ifelse(is.na(length), "NK", length)) %>%
  mutate(no_length_landg=ifelse(no_length_landg > 0 & no_length_landg < 0.5 & no_length_landg!="NK", 1, round(no_length_landg))) %>%
  mutate(no_length_landg=ifelse(is.na(no_length_landg), "NK", no_length_landg)) %>%
  select(country, year, domain_landings, species, totwghtlandg, no_samples_landg, no_length_measurements_landg, lengthunit, min_length, max_length, length, no_length_landg) %>%
  arrange(country, year, domain_landings, species)

tableF=unique(tableF)
tableF=tableF[tableF$length!="NK",]

tableF=tableF %>%
  group_by(country, year, domain_landings, species, no_samples_landg, no_length_measurements_landg, lengthunit, min_length, max_length, length) %>%
  mutate(no_length_landg=as.numeric(no_length_landg)) %>%
  summarise(totwghtlandg=sum(totwghtlandg), no_length_landg=sum(no_length_landg)) %>%
  ungroup()

##Design and extraction
names(tableF)=toupper(names(tableF))

write.table(x=tableF, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_F_LANDINGS_AT_LENGTH.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Table I : specific effort data by rectangle for 2015, 2016 and 2017 --------

## FAO areas shape file location
FAO_shape_file_path=file.path(path, "4 - Scripts\\3 - Data", "FAO_AREAS_NOCOASTLINE.shp", fsep="\\")

# Common functions
source(file.path(path, "4 - Scripts\\3 - Data", "common-misc.R", fsep="\\"))

## Query
balbaya_tableI_effort_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableI_specific_effort_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
efforts_raw=dbGetQuery(balbaya_con, balbaya_tableI_effort_sqlQry)

## Map coordonates to grid
efforts_raw$rectangle_lat <- as.vector(by(efforts_raw, 
                                          1:nrow(efforts_raw), 
                                          function(x) coordToGrid50(x$v_la_act)))
efforts_raw$rectangle_lon <- as.vector(by(efforts_raw, 
                                          1:nrow(efforts_raw), 
                                          function(x) coordToGrid50(x$v_lo_act)))

## Aggregating by 0.5*0.5 squares
efforts_aggregated=aggregate(x=list(effective_effort=efforts_raw$effective_effort), 
                             by=list(country=efforts_raw$country,
                                     year=efforts_raw$year,
                                     quarter=efforts_raw$quarter,
                                     vessel_length=efforts_raw$vessel_length,
                                     fishing_tech=efforts_raw$fishing_tech,
                                     gear_type=efforts_raw$gear_type,
                                     mesh_size_range=efforts_raw$mesh_size_range,
                                     metier=efforts_raw$metier,
                                     supra_region=efforts_raw$supra_region,
                                     geo_indicator=efforts_raw$geo_indicator,
                                     specon_tech=efforts_raw$specon_tech,
                                     target_assemblage=efforts_raw$target_assemblage,
                                     deep=efforts_raw$deep,
                                     rectangle_lat=efforts_raw$rectangle_lat, 
                                     rectangle_lon=efforts_raw$rectangle_lon),
                             FUN=sum, na.rm=FALSE)

## Addressing FAO zones
## Adding barycenter of squares
efforts_aggregated$barycenter_lat=ifelse(efforts_aggregated$rectangle_lat >=0, efforts_aggregated$rectangle_lat+0.25, efforts_aggregated$rectangle_lat-0.25)
efforts_aggregated$barycenter_lon=ifelse(efforts_aggregated$rectangle_lon >=0, efforts_aggregated$rectangle_lon+0.25, efforts_aggregated$rectangle_lon-0.25)

## Lecture du shapefile des zones FAO
FAOShpLayerList=ogrListLayers(dsn=FAO_shape_file_path)
FAOShpSpDf=readOGR(dsn=FAO_shape_file_path, layer=FAOShpLayerList[1])

## Sélection des zones à différents niveaux hiérarchiques
## On ne garde que les zones 	'MAJOR', 'SUBAREA','DIVISION'
FAOShpSpDf=FAOShpSpDf[FAOShpSpDf@data$F_LEVEL %in% c("MAJOR", "SUBAREA", "DIVISION"),]

## On reconstruit la hierarchie des zones
FAOShpSpDf@data$parent=unlist(lapply(strsplit(x=as.character(FAOShpSpDf@data$F_CODE), 
                                              split=".", 
                                              fixed=TRUE),
                                     function(x) {
                                       if (length(x)==1)  {return(NA)}
                                       if (length(x)==2)  {return(x[1])}
                                       if (length(x)==3)  {return(paste0(x[1], ".", x[2]))}
                                     }))

## On ne garde que les feuilles
FAOShpSpDf=FAOShpSpDf[! FAOShpSpDf@data$F_CODE %in% FAOShpSpDf@data$parent,]

## Position des calées
## Transformation en obj spatial
coordinates(efforts_aggregated)= ~barycenter_lon + barycenter_lat
proj4string(efforts_aggregated)=proj4string(FAOShpSpDf)

## Intersection
geomResult=gCovers(FAOShpSpDf, efforts_aggregated, byid=TRUE)

## Combien de zones FAO par (activite ?)barycentre de) carré 0.5°*0.5° ?
nbZonePerActivity=apply(geomResult, 1, sum)

## On supprime les activite sans zones (s'il y en a)
message(sum(nbZonePerActivity==0), " activities outside all zones")
efforts_aggregated=efforts_aggregated[nbZonePerActivity!=0,]
geomResult=geomResult[nbZonePerActivity!=0,]
nbZonePerActivity=nbZonePerActivity[nbZonePerActivity!=0]

## On attribue les zones aux activites
## Need a row id for that processing
efforts_aggregated$actInd=seq_len(nrow(efforts_aggregated))
efforts_aggregated_secondary=data.frame()
for (currAct in efforts_aggregated$actInd) efforts_aggregated_secondary=rbind(efforts_aggregated_secondary, cbind(actInd=currAct, sub_region=as.character(FAOShpSpDf@data$F_CODE[geomResult[currAct,]])))

## Valeur à ponderer par nombre de zones
efforts_aggregated$effective_effort=efforts_aggregated$effective_effort / nbZonePerActivity

## Union des activités dédoublées et des activités unitaires de la structure initiale
efforts_aggregated=merge(efforts_aggregated_secondary, efforts_aggregated, by=c("actInd"))

## Mapping from standard FAO zones to custom DG MARE zones
efforts_aggregated$sub_region=as.character(efforts_aggregated$sub_region)
efforts_aggregated$sub_region[efforts_aggregated$sub_region %in% c("34.2")]="34.2.0"
efforts_aggregated$sub_region[efforts_aggregated$sub_region %in% c("47.1.1", "47.1.2", "47.1.3", "41.1.4")]="47.1"
efforts_aggregated$sub_region[efforts_aggregated$sub_region %in% c("47.A.0", "47.A.1")]="47.A"
efforts_aggregated$sub_region[efforts_aggregated$sub_region %in% c("47.B.1")]="47.B"

efforts_aggregated$eez.indicator=ifelse(test=as.character(efforts_aggregated$sub_region) %in% c("34.1.1"), yes="COAST", no=as.character(efforts_aggregated$eez.indicator))
efforts_aggregated$eez.indicator=ifelse(test=as.character(efforts_aggregated$sub_region) %in% c("34.1.2", "34.1.3", "34.2.0", "27.8.d", "27.9.b"), yes="RFMO", no=as.character(efforts_aggregated$eez.indicator))
efforts_aggregated$eez.indicator=ifelse(test=as.character(efforts_aggregated$sub_region) %in% c("34.3.1", 
                                                                                                "34.3.2", 
                                                                                                "34.3.3", 
                                                                                                "34.3.4", 
                                                                                                "34.3.5", 
                                                                                                "34.3.6", 
                                                                                                "34.4.1", 
                                                                                                "34.4.2",
                                                                                                "27.8.a",
                                                                                                "27.8.c",
                                                                                                "27.9.a",
                                                                                                "47.1",
                                                                                                "47.A",
                                                                                                "47.B",
                                                                                                "57.1",
                                                                                                "57.2",
                                                                                                "51.3",
                                                                                                "51.4",
                                                                                                "51.5",
                                                                                                "51.6",
                                                                                                "51.7"),  
                                        yes="NA", 
                                        no=as.character(efforts_aggregated$eez.indicator))
## Finalizing work
## Erasing legacy data
efforts_aggregated_secondary=NULL

## Providing center of squares, not corner's coordonates
efforts_aggregated$rectangle_lat=efforts_aggregated$barycenter_lat
efforts_aggregated$rectangle_lon=efforts_aggregated$barycenter_lon

## Ceiling/rounding effort column (integer, no 0, -1 if no data)
efforts_aggregated$effective_effort=ifelse(efforts_aggregated$effective_effort <= 0, 0, ifelse(efforts_aggregated$effective_effort <= 1, 1, round(efforts_aggregated$effective_effort, 0)))

## Add last colums
efforts_aggregated$rectangle_type="05*05"
efforts_aggregated$confidential="N"

## Filtering & re ordering columns
tableI=efforts_aggregated %>%
  select(country, 
         year, 
         quarter, 
         vessel_length, 
         fishing_tech, 
         gear_type, 
         mesh_size_range, 
         metier,
         supra_region,
         sub_region,
         eez.indicator,
         geo_indicator,
         specon_tech,
         target_assemblage,
         deep,
         rectangle_type,
         rectangle_lat,
         rectangle_lon,
         effective_effort,
         confidential) %>%
  arrange(country, year, quarter, vessel_length, fishing_tech, sub_region)

## Renaming columns to uppercase
names(tableI)=toupper(names(tableI))

## Writing file
write.table(x=tableI, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_I_SPATIAL_EFFORT.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Table H : Landings data by rectangle for 2015, 2016 and 2017 ------------

## Query
balbaya_tableH_landing_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableH_landing_rectangle_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
balbaya_tableH_landingDf=dbGetQuery(balbaya_con, balbaya_tableH_landing_sqlQry)

## Aggregating activities on 0.5°*0.5° grid
## Map coordonates to grid
balbaya_tableH_landingDf$rectangle_lat=as.vector(by(balbaya_tableH_landingDf, 
                                                    1:nrow(balbaya_tableH_landingDf), 
                                                    function(x) coordToGrid50(x$latitude)))
balbaya_tableH_landingDf$rectangle_lon=as.vector(by(balbaya_tableH_landingDf, 
                                                    1:nrow(balbaya_tableH_landingDf), 
                                                    function(x) coordToGrid50(x$longitude)))

## Aggregating by 0.5*0.5 squares
tableH=aggregate(x=list(totwghtlandg=balbaya_tableH_landingDf$totwghtlandg), 
                        by=list(country=balbaya_tableH_landingDf$country,
                                year=balbaya_tableH_landingDf$year,
                                quarter=balbaya_tableH_landingDf$quarter,
                                vessel_length=balbaya_tableH_landingDf$vessel_length,
                                fishing_tech=balbaya_tableH_landingDf$fishing_tech,
                                gear_type=balbaya_tableH_landingDf$gear_type,
                                mesh_size_range=balbaya_tableH_landingDf$mesh_size_range,
                                metier=balbaya_tableH_landingDf$metier,
                                supra_region=balbaya_tableH_landingDf$supra_region,
                                sub_region=balbaya_tableH_landingDf$sub_region,
                                eez_indicator=balbaya_tableH_landingDf$eez_indicator,
                                geo_indicator=balbaya_tableH_landingDf$geo_indicator,
                                specon_tech=balbaya_tableH_landingDf$specon_tech,
                                target_assemblage=balbaya_tableH_landingDf$target_assemblage,
                                deep=balbaya_tableH_landingDf$deep,
                                rectangle_lat=balbaya_tableH_landingDf$rectangle_lat, 
                                rectangle_lon=balbaya_tableH_landingDf$rectangle_lon,
                                species=balbaya_tableH_landingDf$species,
                                confidential=balbaya_tableH_landingDf$confidential,
                                rectangle_type=balbaya_tableH_landingDf$rectangle_type,
                                totvallandg=balbaya_tableH_landingDf$totvallandg), # to be switeched to variable if data is provided
                        FUN=sum, na.rm=FALSE)

## Finalizing work
## For providing center of squares, not corner's coordonates
tableH$rectangle_lat=ifelse(tableH$rectangle_lat >=0, tableH$rectangle_lat+0.25, tableH$rectangle_lat-0.25)
tableH$rectangle_lon=ifelse(tableH$rectangle_lon >=0, tableH$rectangle_lon+0.25, tableH$rectangle_lon-0.25)

## Re ordering columns
tableH=tableH %>%
  select(country, year, quarter, vessel_length, fishing_tech, gear_type, mesh_size_range, metier, supra_region, sub_region, eez_indicator, geo_indicator, specon_tech, target_assemblage, deep, rectangle_type, rectangle_lat, rectangle_lon, species, totwghtlandg, totvallandg, confidential) %>%
  arrange(year, quarter, vessel_length, fishing_tech, sub_region, eez_indicator, species)

## Renaming columns to uppercase
names(tableH)=toupper(names(tableH))

## Writing file
write.table(x=tableH, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_H_SPATIAL_LANDINGS.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Table G : Effort data for 2015, 2016 and 2017 ---------------------------

balbaya_tableG_effort_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableG_effort_data_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
efforts_raw=dbGetQuery(balbaya_con, balbaya_tableG_effort_sqlQry)

## Transformation en obj spatial
coordinates(efforts_raw)=~v_lo_act + v_la_act
proj4string(efforts_raw)=proj4string(FAOShpSpDf)

## Intersection
geomResult=gCovers(FAOShpSpDf, efforts_raw, byid=TRUE)

## Combien de zone par activite ?
nbZonePerActivity=apply(geomResult, 1, sum)

## On supprime les activite sans zones (s'il y en a)
message(sum(nbZonePerActivity==0), " activities outside all zones")
efforts_raw=efforts_raw[nbZonePerActivity!=0,]
geomResult=geomResult[nbZonePerActivity!=0,]
nbZonePerActivity=nbZonePerActivity[nbZonePerActivity!=0]

## On attribue les zones aux activites
efforts_raw$actInd=seq_len(nrow(efforts_raw))
efforts_aggregated=data.frame()
for (currAct in efforts_raw$actInd) efforts_aggregated=rbind(efforts_aggregated, cbind(actInd=currAct, FAO_zone=as.character(FAOShpSpDf@data$F_CODE[geomResult[currAct,]])))

## Valeur pondere par nombre de zone
efforts_raw$totseadays=efforts_raw$totseadays / nbZonePerActivity
efforts_raw$totkwdaysatsea=efforts_raw$totkwdaysatsea / nbZonePerActivity
efforts_raw$totgtdaysatsea=efforts_raw$totgtdaysatsea / nbZonePerActivity
efforts_raw$totfishdays=efforts_raw$totfishdays / nbZonePerActivity
efforts_raw$totkwfishdays=efforts_raw$totkwfishdays / nbZonePerActivity
efforts_raw$totgtfishdays=efforts_raw$totgtfishdays / nbZonePerActivity
efforts_raw$hrsea=efforts_raw$hrsea / nbZonePerActivity
efforts_raw$kwhrsea=efforts_raw$kwhrsea / nbZonePerActivity
efforts_raw$gthrsea=efforts_raw$gthrsea / nbZonePerActivity

## Fusion des activités dédoublées et des activités unitaires de la structure initiale
efforts_aggregated=merge(efforts_aggregated, efforts_raw, by=c("actInd"))

## Add variable value of vessels conducting activity
tableG_totvessel=efforts_aggregated %>%
  select(FAO_zone:deep)

colnames(tableG_totvessel)[1]="sub_region"

tableG_totvessel$sub_region=as.character(tableG_totvessel$sub_region)
tableG_totvessel$sub_region[tableG_totvessel$sub_region %in% c("34.2")]="34.2.0"
tableG_totvessel$sub_region[tableG_totvessel$sub_region %in% c("47.1.1", "47.1.2", "47.1.3", "41.1.4")]="47.1"
tableG_totvessel$sub_region[tableG_totvessel$sub_region %in% c("47.A.0", "47.A.1")]="47.A"
tableG_totvessel$sub_region[tableG_totvessel$sub_region %in% c("47.B.1")]="47.B"

tableG_totvessel$eez.indicator=ifelse(test=as.character(tableG_totvessel$sub_region) %in% c("34.1.1"), yes="COAST", no=as.character(tableG_totvessel$eez.indicator))
tableG_totvessel$eez.indicator=ifelse(test=as.character(tableG_totvessel$sub_region) %in% c("34.1.2", "34.1.3", "34.2.0", "27.8.d", "27.9.b"), yes="RFMO", no=as.character(tableG_totvessel$eez.indicator))
tableG_totvessel$eez.indicator=ifelse(test=as.character(tableG_totvessel$sub_region) %in% c("34.3.1", 
                                                                                            "34.3.2", 
                                                                                            "34.3.3", 
                                                                                            "34.3.4", 
                                                                                            "34.3.5", 
                                                                                            "34.3.6", 
                                                                                            "34.4.1", 
                                                                                            "34.4.2",
                                                                                            "27.8.a",
                                                                                            "27.8.c",
                                                                                            "27.9.a",
                                                                                            "47.1",
                                                                                            "47.A",
                                                                                            "47.B",
                                                                                            "57.1",
                                                                                            "57.2",
                                                                                            "51.3",
                                                                                            "51.4",
                                                                                            "51.5",
                                                                                            "51.6",
                                                                                            "51.7"), 
                                      yes="NA", 
                                      no=as.character(tableG_totvessel$eez.indicator))

tableG_totvessel=tableG_totvessel %>%
  group_by(year, quarter, vessel_length, fishing_tech, gear_type, mesh_size_range, metier, supra_region, sub_region, eez.indicator, geo_indicator, specon_tech, target_assemblage, deep) %>%
  summarise(totves=n_distinct(c_bat)) %>%
  ungroup()
  
## Aggregation par zones FAO
tableG=aggregate(x=list(totseadays=efforts_aggregated$totseadays, 
                       totkwdaysatsea=efforts_aggregated$totkwdaysatsea,
                       totgtdaysatsea=efforts_aggregated$totgtdaysatsea,
                       totfishdays=efforts_aggregated$totfishdays,
                       totkwfishdays=efforts_aggregated$totkwfishdays,
                       totgtfishdays=efforts_aggregated$totgtfishdays,
                       hrsea=efforts_aggregated$hrsea,
                       kwhrsea=efforts_aggregated$kwhrsea,
                       gthrsea=efforts_aggregated$gthrsea), 
                 by=list(country=efforts_aggregated$country,
                         year=efforts_aggregated$year,
                         quarter=efforts_aggregated$quarter,
                         vessel_length=efforts_aggregated$vessel_length,
                         fishing_tech=efforts_aggregated$fishing_tech,
                         gear_type=efforts_aggregated$gear_type,
                         mesh_size_range=efforts_aggregated$mesh_size_range,
                         metier=efforts_aggregated$metier,
                         supra_region=efforts_aggregated$supra_region,
                         sub_region=efforts_aggregated$FAO_zone,
                         geo_indicator=efforts_aggregated$geo_indicator,
                         specon_tech=efforts_aggregated$specon_tech,
                         target_assemblage=efforts_aggregated$target_assemblage,
                         deep=efforts_aggregated$deep,
                         confidential=efforts_aggregated$confidential
                         ), sum)

## Mapping from standard FAO zones to custom DG MARE zones
tableG$sub_region=as.character(tableG$sub_region)
tableG$sub_region[tableG$sub_region %in% c("34.2")]="34.2.0"
tableG$sub_region[tableG$sub_region %in% c("47.1.1", "47.1.2", "47.1.3", "41.1.4")]="47.1"
tableG$sub_region[tableG$sub_region %in% c("47.A.0", "47.A.1")]="47.A"
tableG$sub_region[tableG$sub_region %in% c("47.B.1")]="47.B"

tableG$eez.indicator=ifelse(test=as.character(tableG$sub_region) %in% c("34.1.1"), yes="COAST", no=as.character(tableG$eez.indicator))
tableG$eez.indicator=ifelse(test=as.character(tableG$sub_region) %in% c("34.1.2", "34.1.3", "34.2.0", "27.8.d", "27.9.b"), yes="RFMO", no=as.character(tableG$eez.indicator))
tableG$eez.indicator=ifelse(test=as.character(tableG$sub_region) %in% c("34.3.1", 
                                                                        "34.3.2", 
                                                                        "34.3.3", 
                                                                        "34.3.4", 
                                                                        "34.3.5", 
                                                                        "34.3.6", 
                                                                        "34.4.1", 
                                                                        "34.4.2",
                                                                        "27.8.a",
                                                                        "27.8.c",
                                                                        "27.9.a",
                                                                        "47.1",
                                                                        "47.A",
                                                                        "47.B",
                                                                        "57.1",
                                                                        "57.2",
                                                                        "51.3",
                                                                        "51.4",
                                                                        "51.5",
                                                                        "51.6",
                                                                        "51.7"), 
                            yes="NA", 
                            no=as.character(tableG$eez.indicator))
## Finishing work
## Management of NA values
tableG$totkwfishdays=ifelse(is.na(tableG$totkwfishdays), "NK", tableG$totkwfishdays)
tableG$totkwdaysatsea=ifelse(is.na(tableG$totkwdaysatsea), "NK", tableG$totkwdaysatsea)
tableG$totgtdaysatsea=ifelse(is.na(tableG$totgtdaysatsea), "NK", tableG$totgtdaysatsea)
tableG$totfishdays=ifelse(is.na(tableG$totfishdays), "NK", tableG$totfishdays)
tableG$totkwfishdays=ifelse(is.na(tableG$totkwfishdays), "NK", tableG$totkwfishdays)
tableG$totgtfishdays=ifelse(is.na(tableG$totgtfishdays), "NK", tableG$totgtfishdays)
tableG$hrsea=ifelse(is.na(tableG$hrsea), "NK", tableG$hrsea)
tableG$kwhrsea=ifelse(is.na(tableG$kwhrsea), "NK", tableG$kwhrsea)
tableG$gthrsea=ifelse(is.na(tableG$gthrsea), "NK", tableG$gthrsea)

## Add number of vessels conducting activity
tableG=tableG %>%
  left_join(tableG_totvessel)

## Final design
tableG=tableG %>%
  select(country:sub_region,
         eez.indicator,
         geo_indicator:deep,
         totseadays:gthrsea,
         totves,
         confidential) %>%
  arrange(country:fishing_tech, sub_region)

names(tableG)=toupper(names(tableG))

## Writing file
write.table(x=tableG, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_G_EFFORT.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Table J : capacity and fleet effort data for 2015, 2016 and 2017 --------

## Loading data
balbaya_tableJ_capacity_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableJ_capacity_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
capacities_raw=dbGetQuery(balbaya_con, balbaya_tableJ_capacity_sqlQry)
capacities_raw=capacities_raw[!(is.na(capacities_raw$fishing_tech)),]

balbaya_tableJ_maxseadays_sqlQry=paste(readLines(con=file.path(path, "4 - Scripts\\2 - SQL", "tableJ_maxseadays_2015_2016_2017_balbaya.sql", fsep="\\")), collapse="\n")
efforts_raw=dbGetQuery(balbaya_con, balbaya_tableJ_maxseadays_sqlQry)

# Ensuring order( we don't trust the query)
efforts_raw <- efforts_raw[order(efforts_raw$country, 
                                 efforts_raw$year, 
                                 efforts_raw$vessel_length, 
                                 efforts_raw$fishing_tech, 
                                 efforts_raw$supra_region, 
                                 efforts_raw$geo_indicator, 
                                 -efforts_raw$maxseadays),]

efforts_raw_HOK=efforts_raw[efforts_raw$fishing_tech=="HOK",]
efforts_raw_HOK=efforts_raw_HOK %>%
  select(-c_bat)
efforts_raw_PS=efforts_raw[efforts_raw$fishing_tech=="PS",]

# Ranking total sea days of each vessel, by strata
efforts_raw_PS$rank <- unlist(tapply(efforts_raw_PS$maxseadays, list(efforts_raw_PS$country, 
                                                                     efforts_raw_PS$year, 
                                                                     efforts_raw_PS$vessel_length, 
                                                                     efforts_raw_PS$fishing_tech, 
                                                                     efforts_raw_PS$supra_region, 
                                                                     efforts_raw_PS$geo_indicator), function(x) rank(-x)))

# Deleting vessels with rank >10
efforts_raw_PS <- subset(efforts_raw_PS, rank<=10)

#Aggregating (mean) by strata, among remaining vessels
efforts_raw_PS <- aggregate(x=list(maxseadays=efforts_raw_PS$maxseadays),
                         by=list(country=efforts_raw_PS$country,
                                 year=efforts_raw_PS$year,
                                 vessel_length=efforts_raw_PS$vessel_length,
                                 fishing_tech=efforts_raw_PS$fishing_tech,
                                 supra_region=efforts_raw_PS$supra_region,
                                 geo_indicator=efforts_raw_PS$geo_indicator),
                         FUN=mean, na.rm=FALSE)

# Merging capacities & efforts
efforts_raw=rbind(efforts_raw_HOK, efforts_raw_PS)

tableJ_tottrips=capacities_raw %>%
  group_by(country, year, vessel_length, fishing_tech, supra_region, geo_indicator) %>%
  summarise(tottrips=n()) %>% 
  ungroup()

capacities=unique(capacities_raw[,c("country", "year", "vessel_length", "fishing_tech", "supra_region", "geo_indicator", "vessel_code", "vessel_kw", "vessel_gt", "vessel_age", "vessel_length_m")]) %>%
  group_by(country, year, vessel_length, fishing_tech, supra_region, geo_indicator) %>%
  summarise(totkw=sum(vessel_kw),
            totgt=sum(vessel_gt),
            totves=n(),
            avgage=mean(vessel_age),
            avgloa=mean(vessel_length_m)) %>%
  mutate(totkw=as.character(totkw)) %>%
  mutate(totkw=ifelse(is.na(totkw), "NK", totkw)) %>%
  ungroup()

# Renaming columns to uppercase
capacities_and_efforts=capacities %>%
  full_join(tableJ_tottrips) %>%
  full_join(efforts_raw) %>%
  select(country, year, vessel_length, fishing_tech, supra_region, geo_indicator, tottrips, totkw, totgt, totves, avgage, avgloa, maxseadays)
  
names(capacities_and_efforts)=toupper(names(capacities_and_efforts))

## Writing file
write.table(x=capacities_and_efforts, 
            file=file.path(path, "5 - Sorties", "FRA_TABLE_J_CAPACITY.csv", fsep="\\"), 
            row.names=FALSE, 
            col.names=TRUE, 
            sep=",",
            dec=".")

# Verifications -----------------------------------------------------------

tableA_sum_allspecies=tableA_catch %>%
  group_by(COUNTRY, YEAR) %>%
  summarise(tableA_TOTWGHTLANDG=sum(TOTWGHTLANDG), tableA_UNWANTED_CATCH=sum(UNWANTED_CATCH)) %>%
  ungroup()

##456,499 of discard for 2017 (BET, YFT and SKJ) with extrapolation method of P. Sabarros
##98262,342 catch (BET, YFT and SKJ) declared by L. Floch's in 2015 at ICCAT/CTOI 
##117992,786 catch (BET, YFT and SKJ) declared by L. Floch's in 2015 at ICCAT/CTOI 
##112972,794 catch (BET, YFT and SKJ) declared by L. Floch's in 2015 at ICCAT/CTOI 

tableH_summary=tableH %>%
  group_by(COUNTRY, YEAR) %>%
  summarise(tableH_TOTWGHTLANDG=sum(TOTWGHTLANDG)) %>%
  ungroup()
            
tableD_summary=unique(tableD[,c("COUNTRY", "YEAR", "DOMAIN_DISCARDS", "SPECIES", "UNWANTED_CATCH", "TOTWGHTLANDG")]) %>%
  group_by(COUNTRY, YEAR) %>%
  summarise(tableD_UNWANTED_CATCH=sum(UNWANTED_CATCH), tableD_TOTWGHTLANDG=sum(TOTWGHTLANDG)) %>%
  ungroup()

table_summary=tableA_sum_allspecies %>%
  full_join(tableH_summary) %>%
  full_join(tableD_summary)
