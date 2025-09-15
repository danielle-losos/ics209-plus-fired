// Google Earth Engine Script to Extract MCD64A1 Burn Date Data
// for East Troublesome Fire (2020)
// 
// This script downloads MODIS MCD64A1 burn date data for the East Troublesome Fire
// area in Colorado for the 2020 fire season.
//
// APPROACH: Uses MTBS fire perimeters with 3km buffer
// - Loads USFS MTBS burned area boundaries
// - Searches for East Troublesome Fire in 2020
// - Creates 3km buffer around MTBS perimeter
//
// Dataset: USFS/GTAC/MTBS/burned_area_boundaries/v1

// ============================================================================
// CONFIGURATION
// ============================================================================

// East Troublesome Fire details
var fireName = 'East_Troublesome_Fire';
var fireYear = 2020;

// Load MTBS burned area boundaries
var mtbs = ee.FeatureCollection("USFS/GTAC/MTBS/burned_area_boundaries/v1");

// Filter for East Troublesome Fire using Event_ID
var eastTroublesomeEventID = "CO4020310623920201014";
var eastTroublesomeFire = mtbs
  .filter(ee.Filter.eq('Event_ID', eastTroublesomeEventID));

print('MTBS fires found for East Troublesome:', eastTroublesomeFire.size());
print('East Troublesome Fire details:', eastTroublesomeFire);

// Debug: Show all 2020 Colorado fires for reference
var coloradoFires2020 = mtbs
  .filter(ee.Filter.eq('Fire_Year', fireYear))
  .filterBounds(ee.Geometry.Rectangle([-109, 37, -102, 41])); // Colorado bounds

print('All Colorado fires in 2020 (first 10):', coloradoFires2020.limit(10));

// Backup search by fire name if Event_ID fails
var alternativeSearch = mtbs
  .filter(ee.Filter.and(
    ee.Filter.eq('Fire_Year', fireYear),
    ee.Filter.or(
      ee.Filter.stringContains('Fire_Name', 'TROUBLESOME'),
      ee.Filter.stringContains('Incid_Name', 'TROUBLESOME')
    )
  ));

print('Alternative search by name:', alternativeSearch);

// Create study area with 3km buffer around MTBS perimeter
var bufferDistance = 3000; // 3 km buffer

// Check if MTBS fire was found and create appropriate study area
var mtbsFireFound = eastTroublesomeFire.size().gt(0);

// Create study area based on whether MTBS fire was found
var mtbsStudyArea = eastTroublesomeFire.geometry().buffer(bufferDistance);

// Fallback: if MTBS fire not found, use ignition point approach
var fallbackPoint = ee.Geometry.Point([-105.8680114589377, 40.28124999635872]);
var fallbackStudyArea = fallbackPoint.buffer(15000); // 15 km radius

// Use appropriate study area
var studyArea = ee.Algorithms.If(mtbsFireFound, mtbsStudyArea, fallbackStudyArea);

// For visualization, we need separate handling
var studyAreaForViz = ee.Algorithms.If(mtbsFireFound, mtbsStudyArea, fallbackStudyArea);

// Date range for analysis (2020 fire season)
var startDate = ee.Date('2020-01-01');
var endDate = ee.Date('2020-12-31');

// ============================================================================
// DATA LOADING AND PROCESSING
// ============================================================================

// Load MCD64A1 ImageCollection
var mcd64a1 = ee.ImageCollection("MODIS/061/MCD64A1");

// Filter collection for study area and time period
var burnDate = mcd64a1
  .filterBounds(studyArea)
  .filterDate(startDate, endDate)
  .select('BurnDate'); // Select only the burn date band

print('Number of MCD64A1 images found:', burnDate.size());

// Create annual composite for 2020
// MCD64A1 provides monthly data, so we need to mosaic/composite
// Force integer values to remove any decimal artifacts
var annualBurnDate = burnDate
  .reduce(ee.Reducer.firstNonNull()) // Takes first non-null value
  .round() // Round to nearest integer
  .toInt16() // Force to 16-bit integer
  .rename('BurnDate') // Rename the band back to BurnDate
  .clip(studyArea);

// Debug: Check if we have proper integer values after processing
print('Annual burn date data type:', annualBurnDate.getInfo());
print('Min/max after integer conversion:', annualBurnDate.reduceRegion({
  reducer: ee.Reducer.minMax(),
  geometry: studyArea,
  scale: 500,
  maxPixels: 1e6
}));

// ============================================================================
// VISUALIZATION
// ============================================================================

// Add layers to map
var ignitionPoint = ee.Geometry.Point([-105.8680114589377, 40.28124999635872]);

// Add MTBS perimeter and its buffer
Map.addLayer(eastTroublesomeFire, {color: 'blue', fillOpacity: 0.3}, 'MTBS Fire Perimeter');
Map.addLayer(mtbsStudyArea, {color: 'red', fillOpacity: 0.1}, 'MTBS Study Area (3km buffer)');

// Add ignition point for reference
Map.addLayer(ignitionPoint, {color: 'yellow'}, 'Ignition Point');

// Visualization parameters for burn date
var burnVis = {
  min: 275, // Approximate day 275 (early October)
  max: 320, // Approximate day 320 (mid November)
  palette: ['yellow', 'orange', 'red', 'darkred']
};

// Add burn date layer to map
Map.addLayer(annualBurnDate.updateMask(annualBurnDate.gt(0)), burnVis, 'Annual Burn Date');

// Center map on the ignition point (works for both MTBS and fallback scenarios)
Map.centerObject(ignitionPoint, 11);

// ============================================================================
// DATA EXPORT
// ============================================================================

// Export configuration
var exportScale = 500; // Native MCD64A1 resolution
var exportCRS = 'EPSG:4326'; // WGS84 geographic

// Export annual burn date for use with progression-maps.R
Export.image.toDrive({
  image: annualBurnDate,
  description: 'mcd64a1_annual_burndate_y2020_east_troublesome',
  fileNamePrefix: 'mcd64a1_annual_burndate_y2020',
  scale: exportScale,
  region: studyArea,
  crs: exportCRS,
  maxPixels: 1e9,
  fileFormat: 'GeoTIFF'
});

// Export MTBS fire perimeter as geopackage
Export.table.toDrive({
  collection: eastTroublesomeFire,
  description: 'mtbs_east_troublesome_perimeter',
  fileNamePrefix: 'mtbs_east_troublesome_fire_perimeter',
  fileFormat: 'GeoJSON'  // GEE doesn't support geopackage directly, but GeoJSON works well
});

// ============================================================================
// ANALYSIS AND STATISTICS
// ============================================================================

// Calculate burn date statistics
var burnStats = annualBurnDate.reduceRegion({
  reducer: ee.Reducer.minMax(),
  geometry: studyArea,
  scale: exportScale,
  maxPixels: 1e9
});

print('Burn date statistics:', burnStats);

// Debug: Check for proper integer values
// Use ignition point as sample center since studyArea is a ComputedObject
var sampleRegion = ignitionPoint.buffer(1000); // Small sample area around ignition
var sampleValues = annualBurnDate.sample({
  region: sampleRegion,
  scale: 500,
  numPixels: 10
});
print('Sample burn date values (should be integers 288-334):', sampleValues);

// Calculate burned area
var burnedPixels = annualBurnDate.gt(0);
var burnedArea = burnedPixels.multiply(ee.Image.pixelArea()).divide(10000); // Convert to hectares

var totalBurnedArea = burnedArea.reduceRegion({
  reducer: ee.Reducer.sum(),
  geometry: studyArea,
  scale: exportScale,
  maxPixels: 1e9
});

print('Total burned area (hectares):', totalBurnedArea);

// Create burn progression time series (monthly breakdown)
var months = ee.List.sequence(1, 12);

var monthlyBurnArea = months.map(function(month) {
  var monthStart = ee.Date.fromYMD(fireYear, month, 1);
  var monthEnd = monthStart.advance(1, 'month');
  
  var monthlyImages = burnDate
    .filterDate(monthStart, monthEnd);
  
  var monthlyBurned = monthlyImages
    .mosaic()
    .gt(0);
  
  var monthlyArea = monthlyBurned
    .multiply(ee.Image.pixelArea())
    .divide(10000) // Convert to hectares
    .reduceRegion({
      reducer: ee.Reducer.sum(),
      geometry: studyArea,
      scale: exportScale,
      maxPixels: 1e9
    });
  
  return ee.Feature(null, {
    'month': month,
    'burned_area_ha': monthlyArea.get('BurnDate')
  });
});

print('Monthly burn progression:', monthlyBurnArea);

// ============================================================================
// USAGE INSTRUCTIONS
// ============================================================================

print('========================================');
print('EXPORT INSTRUCTIONS:');
print('1. Run this script in Google Earth Engine');
print('2. Check the Tasks tab for export jobs');
print('3. Click "Run" on BOTH export tasks:');
print('   - mcd64a1_annual_burndate_y2020.tif (burn date raster)');
print('   - mtbs_east_troublesome_fire_perimeter.geojson (fire boundary)');
print('4. Files will be saved to your Google Drive');
print('5. Download and place files in appropriate directories:');
print('   - Burn date: data/spatial/raw/modis/mcd64a1/');
print('   - Fire perimeter: data/spatial/raw/mtbs/');
print('6. Convert GeoJSON to geopackage in R if needed');
print('========================================');

// Display fire information
print('Fire Information:');
print('Name:', fireName);
print('Year:', fireYear);
print('Event_ID:', eastTroublesomeEventID);
print('Ignition Point (Reference):', ignitionPoint.coordinates());
print('MTBS Buffer Distance:', bufferDistance, 'meters (3km)');
print('MTBS Fire Found:', eastTroublesomeFire.size().gt(0));

// ============================================================================
// QUALITY CONTROL
// ============================================================================

// Check for data availability
var imageCount = burnDate.size();
var firstImage = ee.Image(burnDate.first());
var lastImage = ee.Image(burnDate.sort('system:time_start', false).first());

print('Quality Control:');
print('Total images in collection:', imageCount);
print('First image date:', ee.Date(firstImage.get('system:time_start')));
print('Last image date:', ee.Date(lastImage.get('system:time_start')));

// Check for valid burn date values
var validPixelCount = annualBurnDate.gt(0).reduceRegion({
  reducer: ee.Reducer.sum(),
  geometry: studyArea,
  scale: exportScale,
  maxPixels: 1e9
});

print('Valid burned pixels:', validPixelCount);
