/**
 * CSV Parser for ADSBX_Includes
 * 
 * This module provides functions to parse CSV data into the format expected by ADSBX_Includes
 */

/**
 * Interface for aircraft data in ADSBX_Includes format
 */
interface AircraftInclude {
    domain: string;
    group: string;
    agency?: string;
    ICAO_hex?: string;
    registration?: string;
    callsign?: string;
    cot_type?: string;
    comments?: string;
}

/**
 * Parse CSV data into an array of aircraft objects
 * Expected CSV format:
 * domain,agency,icao_hex,registration,callsign,group,type,comment
 * 
 * @param csvData The CSV data as a string
 * @returns Array of aircraft objects in the ADSBX_Includes format
 */
export function parseCSV(csvData: string): AircraftInclude[] {
    // Split the CSV into lines
    const lines = csvData.split('\n');
    
    // Get the header line and parse it into column names
    // Make sure to trim whitespace and convert to lowercase for more robust matching
    const header = lines[0].split(',').map(col => col.trim().toLowerCase());
    
    // Log the header for debugging
    console.log(`CSV header columns: ${header.join(', ')}`);
    
    // Initialize the result array
    const result = [];
    
    // Process each data line
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue; // Skip empty lines
        
        // Split the line into values
        const values = line.split(',');
        
        // Create an object with the values mapped to the header columns
        const aircraft: Record<string, string> = {};
        for (let j = 0; j < header.length && j < values.length; j++) {
            const key = header[j].trim();
            const value = values[j].trim();
            
            // Only add non-empty values
            if (value) {
                aircraft[key] = value;
            }
        }
        
        // Map the CSV columns to the ADSBX_Includes format
        const mappedAircraft: AircraftInclude = {
            domain: aircraft.domain || 'MIL', // Default to MIL if not specified
            group: aircraft.group || 'UNKNOWN' // Default to UNKNOWN if not specified
        };
        
        // Add optional fields if they exist
        if (aircraft.agency) mappedAircraft.agency = aircraft.agency;
        if (aircraft.icao_hex) mappedAircraft.ICAO_hex = aircraft.icao_hex;
        if (aircraft.registration) mappedAircraft.registration = aircraft.registration;
        if (aircraft.callsign) mappedAircraft.callsign = aircraft.callsign;
        if (aircraft.cot_type) mappedAircraft.cot_type = aircraft.cot_type;
        if (aircraft.type) mappedAircraft.cot_type = aircraft.type; // Map 'type' to 'cot_type'
        if (aircraft.comments) mappedAircraft.comments = aircraft.comments;
        if (aircraft.comment) mappedAircraft.comments = aircraft.comment; // Map 'comment' to 'comments'
        
        // Add the aircraft to the result array
        result.push(mappedAircraft);
    }
    
    return result;
}