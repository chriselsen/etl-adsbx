/**
 * Helper functions for Google Drive downloads
 */

import { fetch } from '@tak-ps/etl';

/**
 * Extract Google Drive file ID from various URL formats
 * @param url Google Drive URL
 * @returns File ID or empty string if not found
 */
export function extractGoogleDriveFileId(url: string): string {
    if (!url.includes('drive.google.com')) {
        return '';
    }
    
    let fileId = '';
    
    if (url.includes('id=')) {
        // Format: https://drive.google.com/uc?id=FILE_ID
        fileId = url.split('id=')[1].split('&')[0];
    } else if (url.includes('/file/d/')) {
        // Format: https://drive.google.com/file/d/FILE_ID/view?usp=sharing
        fileId = url.split('/file/d/')[1].split('/')[0];
    } else if (url.includes('/d/')) {
        // Alternative format: https://drive.google.com/d/FILE_ID/
        fileId = url.split('/d/')[1].split('/')[0];
    }
    
    return fileId;
}

/**
 * Try to download a file from Google Drive using multiple methods
 * @param url Original Google Drive URL
 * @returns CSV data as string
 */
export async function downloadFromGoogleDrive(url: string): Promise<string> {
    const fileId = extractGoogleDriveFileId(url);
    if (!fileId) {
        throw new Error('Could not extract file ID from Google Drive URL');
    }
    
    console.log(`[GDrive] Extracted file ID: ${fileId}`);
    
    // Try multiple download methods in sequence
    const downloadMethods = [
        // Method 1: Standard export download
        {
            url: `https://drive.google.com/uc?export=download&id=${fileId}`,
            description: 'Standard export download'
        },
        // Method 2: Google Sheets export (if it's a spreadsheet)
        {
            url: `https://docs.google.com/spreadsheets/d/${fileId}/export?format=csv`,
            description: 'Google Sheets export'
        },
        // Method 3: Alternative export format
        {
            url: `https://drive.google.com/uc?id=${fileId}&export=download`,
            description: 'Alternative export format'
        }
    ];
    
    let lastError = null;
    
    // Try each method in sequence
    for (const method of downloadMethods) {
        try {
            console.log(`[GDrive] Trying method: ${method.description} - ${method.url}`);
            
            const response = await fetch(method.url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
                }
            });
            
            console.log(`[GDrive] Response status: ${response.status} ${response.statusText}`);
            
            if (!response.ok) {
                throw new Error(`HTTP error: ${response.status} ${response.statusText}`);
            }
            
            const data = await response.text();
            console.log(`[GDrive] Received data length: ${data.length} characters`);
            
            if (data && data.trim().length > 0) {
                return data;
            } else {
                throw new Error('Received empty data');
            }
        } catch (error) {
            console.log(`[GDrive] Method failed: ${error.message}`);
            lastError = error;
            // Continue to the next method
        }
    }
    
    // If we get here, all methods failed
    throw lastError || new Error('All download methods failed');
}