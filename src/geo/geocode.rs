// Constants for EPSG:3857 (Web Mercator projection) coordinate ranges
const MIN_LATITUDE: f64 = -85.051_128_78;
const MAX_LATITUDE: f64 = 85.051_128_78;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

// Earth's quadratic mean radius for WGS-84 in meters
const EARTH_RADIUS_IN_METERS: f64 = 6_372_797.560_856;
const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

/// Validates that the given latitude and longitude are within the valid ranges
/// for EPSG:3857 (Web Mercator projection) as used by Redis.
///
/// Valid longitudes: -180° to +180° (inclusive)
/// Valid latitudes: -85.05112878° to +85.05112878° (inclusive)
///
#[must_use]
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
fn get_lat_distance(lat1_deg: f64, lat2_deg: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2_deg) - deg_rad(lat1_deg)).abs()
}

/// Calculate distance using haversine great circle distance formula.
/// This is the main distance calculation function used by Redis GEODIST.
#[must_use]
pub fn get_distance(lon1_deg: f64, lat1_deg: f64, lon2_deg: f64, lat2_deg: f64) -> f64 {
    let lon1_rad = deg_rad(lon1_deg);
    let lon2_rad = deg_rad(lon2_deg);
    let v = ((lon2_rad - lon1_rad) / 2.0).sin();

    // If v == 0 we can avoid doing expensive math when lons are practically the same
    if v == 0.0 {
        return get_lat_distance(lat1_deg, lat2_deg);
    }

    let lat1_rad = deg_rad(lat1_deg);
    let lat2_rad = deg_rad(lat2_deg);
    let u = ((lat2_rad - lat1_rad) / 2.0).sin();
    let a = u * u + lat1_rad.cos() * lat2_rad.cos() * v * v;

    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn encode(latitude: f64, longitude: f64) -> u64 {
    // Normalize to the range 0-2^26
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Truncate to integers
    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}

#[must_use]
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
    let mut result = u64::from(v);
    result = (result | (result << 16)) & 0x0000_FFFF_0000_FFFF;
    result = (result | (result << 8)) & 0x00FF_00FF_00FF_00FF;
    result = (result | (result << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    result = (result | (result << 2)) & 0x3333_3333_3333_3333;
    (result | (result << 1)) & 0x5555_5555_5555_5555
}

/// Compacts a 64-bit integer back to 32-bit by removing interleaved zeros.
#[allow(clippy::cast_possible_truncation)]
fn compact_int64_to_int32(v: u64) -> u32 {
    let mut result = v & 0x5555_5555_5555_5555;
    result = (result | (result >> 1)) & 0x3333_3333_3333_3333;
    result = (result | (result >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    result = (result | (result >> 4)) & 0x00FF_00FF_00FF_00FF;
    result = (result | (result >> 8)) & 0x0000_FFFF_0000_FFFF;
    ((result | (result >> 16)) & 0x0000_0000_FFFF_FFFF) as u32 // Cast to u32
}

fn convert_grid_numbers_to_coordinates(
    grid_latitude_number: u32,
    grid_longitude_number: u32,
) -> (f64, f64) {
    // Calculate the grid boundaries
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (f64::from(grid_latitude_number) / 2.0_f64.powi(26));
    let grid_latitude_max =
        MIN_LATITUDE + LATITUDE_RANGE * (f64::from(grid_latitude_number + 1) / 2.0_f64.powi(26));
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (f64::from(grid_longitude_number) / 2.0_f64.powi(26));
    let grid_longitude_max =
        MIN_LONGITUDE + LONGITUDE_RANGE * (f64::from(grid_longitude_number + 1) / 2.0_f64.powi(26));

    // Calculate the center point of the grid cell
    let latitude = f64::midpoint(grid_latitude_min, grid_latitude_max);
    let longitude = f64::midpoint(grid_longitude_min, grid_longitude_max);

    (latitude, longitude)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_coordinates() {
        // Test valid coordinates at boundaries
        assert!(is_valid_coordinate(-85.051_128_78, -180.0));
        assert!(is_valid_coordinate(85.051_128_78, 180.0));
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
        assert!(!is_valid_coordinate(85.051_128_79, 0.0));

        // Test invalid latitude (too low)
        assert!(!is_valid_coordinate(-90.0, 0.0));
        assert!(!is_valid_coordinate(-85.051_128_79, 0.0));

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
        assert!(distance > 5_500_000.0 && distance < 5_600_000.0);

        // Test distance between same points (should be 0)
        let same_point_distance = get_distance(ny_lon, ny_lat, ny_lon, ny_lat);
        assert!(same_point_distance < 1.0); // Should be very close to 0

        // Test latitude-only distance
        let lat_distance = get_lat_distance(ny_lat, london_lat);
        assert!(lat_distance > 1_000_000.0 && lat_distance < 2_000_000.0);
    }
}
