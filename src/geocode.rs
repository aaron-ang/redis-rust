// Constants for EPSG:3857 (Web Mercator projection) coordinate ranges
const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

// Earth's quadratic mean radius for WGS-84 in meters
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

/// Validates that the given latitude and longitude are within the valid ranges
/// for EPSG:3857 (Web Mercator projection) as used by Redis.
///
/// Valid longitudes: -180° to +180° (inclusive)
/// Valid latitudes: -85.05112878° to +85.05112878° (inclusive)
///
pub fn is_valid_coordinate(latitude: f64, longitude: f64) -> bool {
    (MIN_LATITUDE..=MAX_LATITUDE).contains(&latitude)
        && (MIN_LONGITUDE..=MAX_LONGITUDE).contains(&longitude)
}

fn deg_rad(ang: f64) -> f64 {
    ang * DEG_TO_RAD
}

/// Calculate distance using simplified haversine great circle distance formula.
/// Given longitude diff is 0 the asin(sqrt(a)) on the haversine is asin(sin(abs(u))).
/// arcsin(sin(x)) equal to x when x ∈[−π/2,π/2]. Given latitude is between [−π/2,π/2]
/// we can simplify arcsin(sin(x)) to x.
fn get_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2d) - deg_rad(lat1d)).abs()
}

/// Calculate distance using haversine great circle distance formula.
/// This is the main distance calculation function used by Redis GEODIST.
pub fn get_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = deg_rad(lon1d);
    let lon2r = deg_rad(lon2d);
    let v = ((lon2r - lon1r) / 2.0).sin();

    // If v == 0 we can avoid doing expensive math when lons are practically the same
    if v == 0.0 {
        return get_lat_distance(lat1d, lat2d);
    }

    let lat1r = deg_rad(lat1d);
    let lat2r = deg_rad(lat2d);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;

    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

pub fn encode(latitude: f64, longitude: f64) -> u64 {
    // Normalize to the range 0-2^26
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Truncate to integers
    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}

pub fn decode(geo_code: u64) -> (f64, f64) {
    // Align bits of both latitude and longitude to take even-numbered position
    let y = geo_code >> 1;
    let x = geo_code;

    // Compact bits back to 32-bit ints
    let grid_latitude_number = compact_int64_to_int32(x);
    let grid_longitude_number = compact_int64_to_int32(y);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

/// Interleaves two 32-bit integers into a 64-bit geohash.
fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

/// Spreads a 32-bit integer to 64-bit by interleaving zeros.
fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

/// Compacts a 64-bit integer back to 32-bit by removing interleaved zeros.
fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555555555555555;
    result = (result | (result >> 1)) & 0x3333333333333333;
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
    ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32 // Cast to u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> (f64, f64) {
    // Calculate the grid boundaries
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));

    // Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    (latitude, longitude)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_coordinates() {
        // Test valid coordinates at boundaries
        assert!(is_valid_coordinate(-85.05112878, -180.0));
        assert!(is_valid_coordinate(85.05112878, 180.0));
        assert!(is_valid_coordinate(0.0, 0.0));

        // Test valid coordinates within range
        assert!(is_valid_coordinate(40.7128, -74.0060)); // New York
        assert!(is_valid_coordinate(51.5074, -0.1278)); // London
        assert!(is_valid_coordinate(-33.8688, 151.2093)); // Sydney
    }

    #[test]
    fn test_invalid_coordinates() {
        // Test invalid latitude (too high)
        assert!(!is_valid_coordinate(90.0, 0.0));
        assert!(!is_valid_coordinate(85.05112879, 0.0));

        // Test invalid latitude (too low)
        assert!(!is_valid_coordinate(-90.0, 0.0));
        assert!(!is_valid_coordinate(-85.05112879, 0.0));

        // Test invalid longitude (too high)
        assert!(!is_valid_coordinate(0.0, 180.1));
        assert!(!is_valid_coordinate(0.0, 200.0));

        // Test invalid longitude (too low)
        assert!(!is_valid_coordinate(0.0, -180.1));
        assert!(!is_valid_coordinate(0.0, -200.0));

        // Test both invalid
        assert!(!is_valid_coordinate(90.0, 200.0));
    }

    #[test]
    fn test_distance_calculation() {
        // Test distance between two points (New York to London)
        let ny_lat = 40.7128;
        let ny_lon = -74.0060;
        let london_lat = 51.5074;
        let london_lon = -0.1278;

        let distance = get_distance(ny_lon, ny_lat, london_lon, london_lat);
        // Expected distance is approximately 5570 km
        assert!(distance > 5500000.0 && distance < 5600000.0);

        // Test distance between same points (should be 0)
        let same_point_distance = get_distance(ny_lon, ny_lat, ny_lon, ny_lat);
        assert!(same_point_distance < 1.0); // Should be very close to 0

        // Test latitude-only distance
        let lat_distance = get_lat_distance(ny_lat, london_lat);
        assert!(lat_distance > 1000000.0 && lat_distance < 2000000.0);
    }
}
