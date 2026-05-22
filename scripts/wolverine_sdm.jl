import Pkg; Pkg.activate(".")
# Pkg.add("SpeciesDistributionToolkit")
using SpeciesDistributionToolkit
const SDT = SpeciesDistributionToolkit
using Statistics
using PrettyTables
using CairoMakie
using ZipArchives


pd = PolygonData(GADM, Countries)
fc = downloader(pd; country = "CAN", level = 1)

aoi = FeatureCollection(filter(f -> f.properties["Name"] == "BritishColumbia", fc.features))

extent = SDT.boundingbox(bc)

chelsa_bioclim = RasterData(CHELSA2, BioClim)
layers(chelsa_bioclim)[1:5]


# get adaptweast data into data folder
aw_url = "https://s3-us-west-2.amazonaws.com/www.cacpd.org/CMIP6v73/normals/Normal_1991_2020_bioclim.zip"

aw_zip_path = joinpath("data", "climate", basename(aw_url))

mkpath(dirname(aw_zip_path))

if !isfile(aw_zip_path)
    download(aw_url, aw_zip_path)
end

aw_uz_fold = replace(aw_zip_path, ".zip" => "")
mkpath(aw_uz_fold)

zip_archive = ZipArchives.ZipReader(read(aw_zip_path))

files = filter(a -> endswith(a, ".tif"), ZipArchives.zip_names(zip_archive))

for file_in_zip in files
    out = open(joinpath(aw_uz_fold, basename(file_in_zip)), "w")
    write(out, ZipArchives.zip_readentry(zip_archive, file_in_zip, String))
    close(out)
end

# i need 
# mean annual precipitation (mm)
# chilling degree days (Degree days below 0 °C)
# precipitation as snow (mm) 
# Hargreave’s climatic moisture index
# warming degree days above 18 °C. 

prefix = replace(basename(aw_url), "_bioclim.zip" => "")

vars = ["MAP", # mean annual precipitation (mm)
"DD_0", # chilling degree days (Degree days below 0 °C)
"PAS", # precipitation as snow (mm) 
"CMD", # Hargreave’s climatic moisture index
"DD18"] # warming degree days above 18 °C. 

map = joinpath(aw_uz_fold, prefix * "_MAP.tif")
CDD = joinpath(aw_uz_fold, prefix * "_MAP.tif")