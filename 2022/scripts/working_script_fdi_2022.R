# setup ----
library(acdc)
library(furdeb)
config <- configuration_file(path_file = "D:\\projets_themes\\data_calls\\fdi\\2022\\data\\fdi_2022_configuration_file.yml",
                             silent = TRUE)
# not apply scientific format for number (may cause problems for cwp maniuplation)
scipen_defaut <- options("scipen")
options("scipen" = 100)
# parameters definition ----
period <- as.integer(x = c(2013:2021))
# 1 = PS, 2 = BB and 3 = LL
gear <- as.integer(x = c(1, 2, 3))
# for the French fleet, 1 = France & 41 = Mayotte
flag <- as.integer(x = c(1, 41))
# checking
template_checking = TRUE
template_year = 2022L
# shapes
fao_area_file_path <- "D:\\developpement\\shapes\\FAO_AREAS_CWP_NOCOASTLINE\\FAO_AREAS_CWP_NOCOASTLINE.Rdata"
eez_area_file_path <- "D:\\developpement\\shapes\\Intersect_EEZ_IHO_v4_2020\\Intersect_EEZ_IHO_v4_2020.Rdata"
cwp_grid_1deg_1deg <- "D:\\developpement\\shapes\\fao_cwp_grid\\cwp-cwp-grid-map-1deg_x_1deg\\cwp-grid-map-1deg_x_1deg.Rdata"
cwp_grid_5deg_5deg <- "D:\\developpement\\shapes\\fao_cwp_grid\\cwp-cwp-grid-map-5deg_x_5deg\\cwp-grid-map-5deg_x_5deg.Rdata"
# csv files locations ----
observe_bycatch_path <- file.path(config[["wd_path"]],
                                  "data",
                                  "by_catch")
observe_discard_path <- file.path(config[["wd_path"]],
                                  "data",
                                  "discards")
# databases connections ----
t3_con <- postgresql_dbconnection(db_user = config[["databases_configuration"]][["t3_prod_vmot7"]]$login,
                                  db_password = config[["databases_configuration"]][["t3_prod_vmot7"]]$password,
                                  db_dbname = config[["databases_configuration"]][["t3_prod_vmot7"]]$dbname,
                                  db_host = config[["databases_configuration"]][["t3_prod_vmot7"]]$host,
                                  db_port = config[["databases_configuration"]][["t3_prod_vmot7"]]$port)

balbaya_con <- postgresql_dbconnection(db_user = config[["databases_configuration"]][["balbaya_vmot5"]]$login,
                                       db_password = config[["databases_configuration"]][["balbaya_vmot5"]]$password,
                                       db_dbname = config[["databases_configuration"]][["balbaya_vmot5"]]$dbname,
                                       db_host = config[["databases_configuration"]][["balbaya_vmot5"]]$host,
                                       db_port = config[["databases_configuration"]][["balbaya_vmot5"]]$port)

sardara_con <- postgresql_dbconnection(db_user = config[["databases_configuration"]][["sardara_vmot5"]]$login,
                                       db_password = config[["databases_configuration"]][["sardara_vmot5"]]$password,
                                       db_dbname = config[["databases_configuration"]][["sardara_vmot5"]]$dbname,
                                       db_host = config[["databases_configuration"]][["sardara_vmot5"]]$host,
                                       db_port = config[["databases_configuration"]][["sardara_vmot5"]]$port)

# processes
tablea <- fdi_tablea_catch_summary(balbaya_con = balbaya_con[[2]],
                                   observe_bycatch_path = observe_bycatch_path,
                                   period = period,
                                   gear = gear,
                                   flag = flag,
                                   fao_area_file_path = fao_area_file_path,
                                   eez_area_file_path = eez_area_file_path,
                                   cwp_grid_file_path = cwp_grid_1deg_1deg,
                                   template_checking = template_checking,
                                   template_year = template_year,
                                   table_export_path = config[["output_path"]])

tabled <- fdi_tabled_discard_length(observe_discard_path = observe_discard_path,
                                    tablea_catch_summary = tablea[["fdi_tables"]][["table_a"]],
                                    template_checking = template_checking,
                                    template_year = template_year,
                                    table_export_path = config[["output_path"]])

tablef <- fdi_tablef_landings_length(balbaya_con = balbaya_con[[2]],
                                     sardara_con = sardara_con[[2]],
                                     t3_con = t3_con[[2]],
                                     period = period,
                                     gear = gear,
                                     flag = flag,
                                     tablea_bycatch_retained = tablea[["ad_hoc_tables"]][["bycatch_retained"]],
                                     tablea_catch_summary = tablea[["fdi_tables"]][["table_a"]],
                                     cwp_grid_file_path = cwp_grid_5deg_5deg,
                                     fao_area_file_path = fao_area_file_path,
                                     template_checking = template_checking,
                                     template_year = template_year,
                                     table_export_path = config[["output_path"]])

tableg <- fdi_tableg_effort(balbaya_con = balbaya_con[[2]],
                            period = period,
                            gear = gear,
                            flag = flag,
                            fao_area_file_path = fao_area_file_path,
                            eez_area_file_path = eez_area_file_path,
                            template_checking = template_checking,
                            template_year = template_year,
                            table_export_path = config[["output_path"]])

tableh <- fdi_tableh_landings_rectangle(tablea_bycatch_retained = tablea[["ad_hoc_tables"]][["bycatch_retained"]],
                                        tablea_landing_rectangle = tablea[["ad_hoc_tables"]][["landing_rectangle"]],
                                        template_checking = template_checking,
                                        template_year = template_year,
                                        table_export_path = config[["output_path"]])

tablei <- fdi_tablei_effort_rectangle(tableg_effort_rectangle = tableg[["ad_hoc_tables"]][["effort_rectangle"]],
                                      template_checking = template_checking,
                                      template_year = template_year,
                                      table_export_path = config[["output_path"]])

tablej <- fdi_tablej_capacity(balbaya_con = balbaya_con[[2]],
                              period = period,
                              gear = gear,
                              flag = flag,
                              fao_area_file_path = fao_area_file_path,
                              template_checking = template_checking,
                              template_year = template_year,
                              table_export_path = config[["output_path"]])

# shortcut function ----
fdi_tables <- fdi_shortcut_function(balbaya_con = balbaya_con[[2]],
                                    sardara_con = sardara_con[[2]],
                                    t3_con = t3_con[[2]],
                                    period = period,
                                    gear = gear,
                                    flag = flag,
                                    observe_bycatch_path = observe_bycatch_path,
                                    observe_discard_path = observe_discard_path,
                                    fao_area_file_path = fao_area_file_path,
                                    eez_area_file_path = eez_area_file_path,
                                    cwp_grid_1deg_1deg = cwp_grid_1deg_1deg,
                                    cwp_grid_5deg_5deg = cwp_grid_5deg_5deg,
                                    template_checking = template_checking,
                                    template_year = template_year,
                                    table_export_path = config[["output_path"]])

# fdi tables consistency
fdi_checks <- fdi_tables_consistency(tablea = fdi_tables$fdi_tables$table_a,
                                     tabled = fdi_tables$fdi_tables$table_d,
                                     tablef = fdi_tables$fdi_tables$table_f,
                                     tableg = fdi_tables$fdi_tables$table_g,
                                     tableh = fdi_tables$fdi_tables$table_h,
                                     tablei = fdi_tables$fdi_tables$table_i,
                                     tablea_bycatch_retained = fdi_tables$ad_hoc_tables$bycatch_retained)
