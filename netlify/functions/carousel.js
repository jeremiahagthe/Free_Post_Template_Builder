const sharp = require('sharp');
const https = require('https');
const http = require('http');
const { URL } = require('url');
const NodeCache = require('node-cache');

// Rate limiting cache (10 requests per minute per IP)
const rateLimitCache = new NodeCache({ stdTTL: 60 });
const RATE_LIMIT = 10;

/**
 * Download image from URL with robust redirect handling
 * Handles HTTP 301, 302, 303, 307, 308 redirects (e.g., Google Drive links)
 */
async function downloadImage(url, maxRedirects = 10, redirectCount = 0) {
  return new Promise((resolve, reject) => {
    // Prevent infinite redirect loops
    if (redirectCount > maxRedirects) {
      reject(new Error(`Too many redirects (max ${maxRedirects}). Possible redirect loop.`));
      return;
    }

    const parsedUrl = new URL(url);
    const protocol = parsedUrl.protocol === 'https:' ? https : http;

    const options = {
      timeout: 10000, // 10 seconds timeout
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; CarouselGenerator/1.0)'
      }
    };

    const request = protocol.get(url, options, (response) => {
      // Handle redirect status codes (301, 302, 303, 307, 308)
      if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
        const location = response.headers.location;
        
        // Consume response body to free resources
        response.on('data', () => {});
        response.on('end', () => {});
        response.destroy();

        // Handle relative and absolute redirect URLs
        let redirectUrl;
        try {
          redirectUrl = new URL(location, url).href;
        } catch (e) {
          reject(new Error(`Invalid redirect URL: ${location}`));
          return;
        }

        // Follow the redirect recursively
        downloadImage(redirectUrl, maxRedirects, redirectCount + 1)
          .then(resolve)
          .catch(reject);
        return;
      }

      // Only accept 200 status code for final response
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download image: HTTP ${response.statusCode} ${response.statusMessage || ''}`));
        return;
      }

      // Validate content type (optional, but helps catch errors early)
      const contentType = response.headers['content-type'];
      if (contentType && !contentType.startsWith('image/')) {
        response.destroy();
        reject(new Error(`Invalid content type: ${contentType}. Expected an image.`));
        return;
      }

      const chunks = [];
      response.on('data', (chunk) => chunks.push(chunk));
      response.on('end', () => {
        if (chunks.length === 0) {
          reject(new Error('Empty response from image server'));
          return;
        }
        resolve(Buffer.concat(chunks));
      });
      response.on('error', reject);
    });

    request.on('error', (error) => {
      reject(new Error(`Network error: ${error.message}`));
    });
    
    request.on('timeout', () => {
      request.destroy();
      reject(new Error(`Image download timeout after ${options.timeout}ms`));
    });

    // Handle connection errors
    request.on('aborted', () => {
      reject(new Error('Image download was aborted'));
    });
  });
}

/**
 * Helper function to wrap text into lines based on max width
 * Uses character width estimation for accurate line breaking
 */
function wrapTextIntoLines(text, maxWidth, fontSize, fontFamily) {
  if (!text || text.trim().length === 0) {
    return [];
  }

  const words = text.split(' ');
  const lines = [];
  let currentLine = '';

  // Character width multipliers based on font type
  // Impact and bold fonts are wider, serif fonts vary
  const charWidthMultipliers = {
    'Impact': 0.65,
    'Arial Black': 0.65,
    'Bebas Neue': 0.55,
    'Arial': 0.55,
    'Helvetica': 0.55,
    'Futura': 0.50,
    'Georgia': 0.55,
    'Times': 0.50,
    'default': 0.55
  };

  const multiplier = charWidthMultipliers[fontFamily] || charWidthMultipliers['default'];

  for (const word of words) {
    const testLine = currentLine ? currentLine + ' ' + word : word;
    const estimatedWidth = testLine.length * fontSize * multiplier;

    if (estimatedWidth > maxWidth && currentLine) {
      lines.push(currentLine);
      currentLine = word;
    } else {
      currentLine = testLine;
    }
  }
  
  if (currentLine) {
    lines.push(currentLine);
  }
  
  return lines.length > 0 ? lines : [text];
}

/**
 * Create text overlay SVG with title and subtitle
 * Uses native SVG text elements with tspan for line wrapping (compatible with Sharp/librsvg)
 */
function createTextSVG({ 
  title, 
  subtitle, 
  textColor = '#FFFFFF', 
  width, 
  height, 
  fontFamily = 'Arial',
  titleSize = null,
  subtitleSize = null,
  titleX = null,
  titleY = null,
  subtitleX = null,
  subtitleY = null,
  maxTitleWidth = null,
  maxSubtitleWidth = null,
  textAlign = 'center'
}) {
  // Calculate default sizes if not provided
  const calculatedTitleSize = titleSize || Math.floor(width * 0.055); // ~60pt for 1080px
  const calculatedSubtitleSize = subtitleSize || Math.floor(width * 0.033); // ~36pt for 1080px
  const padding = Math.floor(width * 0.05);
  const defaultMaxTextWidth = width - (padding * 2);
  
  // Use provided max widths or calculate defaults
  const titleMaxWidth = maxTitleWidth || defaultMaxTextWidth;
  const subtitleMaxWidth = maxSubtitleWidth || defaultMaxTextWidth;

  // Check if we have any text to render
  const hasTitle = title && title.trim().length > 0;
  const hasSubtitle = subtitle && subtitle.trim().length > 0;
  
  // Return empty SVG if no text
  if (!hasTitle && !hasSubtitle) {
    return Buffer.from(`<svg width="${width}" height="${height}"></svg>`);
  }

  // Calculate horizontal positioning
  const titleXPos = titleX !== null && titleX !== undefined ? titleX : width / 2;
  const subtitleXPos = subtitleX !== null && subtitleX !== undefined ? subtitleX : width / 2;
  const textAnchor = textAlign === 'left' ? 'start' : textAlign === 'right' ? 'end' : 'middle';

  // Map font names to font-family CSS strings
  const fontFamilyMap = {
    'Arial': 'Arial, Helvetica, sans-serif',
    'Helvetica': 'Helvetica, Arial, sans-serif',
    'Roboto': 'Roboto, Arial, sans-serif',
    'Open Sans': 'Open Sans, Arial, sans-serif',
    'Montserrat': 'Montserrat, Arial, sans-serif',
    'Bebas Neue': 'Bebas Neue, Impact, Arial, sans-serif',
    'Impact': 'Impact, Arial Black, sans-serif',
    'Futura': 'Futura, Trebuchet MS, Arial, sans-serif',
    'Georgia': 'Georgia, serif',
    'Times': 'Times New Roman, Times, serif'
  };

  const fontFamilyCSS = fontFamilyMap[fontFamily] || fontFamilyMap['Arial'];

  // Wrap text into lines
  const titleLines = hasTitle ? wrapTextIntoLines(title, titleMaxWidth, calculatedTitleSize, fontFamily) : [];
  const subtitleLines = hasSubtitle ? wrapTextIntoLines(subtitle, subtitleMaxWidth, calculatedSubtitleSize, fontFamily) : [];

  // Calculate line heights
  const titleLineHeight = calculatedTitleSize * 1.2;
  const subtitleLineHeight = calculatedSubtitleSize * 1.3;

  // Calculate vertical positioning (use provided or calculate)
  // Adjust for number of lines to center the text block
  let finalTitleY = titleY;
  let finalSubtitleY = subtitleY;
  
  if (finalTitleY === null || finalTitleY === undefined) {
    const titleBlockHeight = titleLines.length * titleLineHeight;
    finalTitleY = height / 2;
    if (hasTitle && hasSubtitle) {
      // Position title above center, accounting for multi-line
      finalTitleY = height * 0.35;
    }
  }
  
  if (finalSubtitleY === null || finalSubtitleY === undefined) {
    const subtitleBlockHeight = subtitleLines.length * subtitleLineHeight;
    finalSubtitleY = height / 2;
    if (hasTitle && hasSubtitle) {
      // Position subtitle below center, accounting for multi-line
      finalSubtitleY = height * 0.65;
    }
  }

  // Generate title tspans
  const titleTspans = titleLines.map((line, index) => {
    if (index === 0) {
      return escapeXml(line);
    }
    return `<tspan x="${titleXPos}" dy="${titleLineHeight}">${escapeXml(line)}</tspan>`;
  }).join('');

  // Generate subtitle tspans
  const subtitleTspans = subtitleLines.map((line, index) => {
    if (index === 0) {
      return escapeXml(line);
    }
    return `<tspan x="${subtitleXPos}" dy="${subtitleLineHeight}">${escapeXml(line)}</tspan>`;
  }).join('');

  // Create SVG with native text elements (compatible with Sharp/librsvg)
  const svg = `<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="2" dy="2" stdDeviation="3" flood-opacity="0.5"/>
    </filter>
  </defs>

  ${hasTitle && titleLines.length > 0 ? `
  <text x="${titleXPos}" y="${finalTitleY}" 
        text-anchor="${textAnchor}"
        font-family="${fontFamilyCSS}"
        font-size="${calculatedTitleSize}"
        font-weight="bold"
        fill="${textColor}"
        stroke="rgba(0,0,0,0.4)"
        stroke-width="2"
        paint-order="stroke fill"
        filter="url(#shadow)">${titleTspans}</text>
  ` : ''}

  ${hasSubtitle && subtitleLines.length > 0 ? `
  <text x="${subtitleXPos}" y="${finalSubtitleY}" 
        text-anchor="${textAnchor}"
        font-family="${fontFamilyCSS}"
        font-size="${calculatedSubtitleSize}"
        font-weight="normal"
        fill="${textColor}"
        stroke="rgba(0,0,0,0.3)"
        stroke-width="1"
        paint-order="stroke fill"
        filter="url(#shadow)">${subtitleTspans}</text>
  ` : ''}
</svg>`;

  return Buffer.from(svg);
}

/**
 * Escape XML special characters
 */
function escapeXml(unsafe) {
  if (!unsafe) return '';
  return String(unsafe).replace(/[<>&'"]/g, (c) => {
    switch (c) {
      case '<': return '&lt;';
      case '>': return '&gt;';
      case '&': return '&amp;';
      case '\'': return '&apos;';
      case '"': return '&quot;';
    }
  });
}

/**
 * Generate single carousel slide
 */
async function generateSlide({ background, slide, width, height, index }) {
  try {
    // Download background image
    const backgroundBuffer = await downloadImage(background);

    // Resize and crop background to target dimensions
    let processedBackground = await sharp(backgroundBuffer)
      .resize(width, height, {
        fit: 'cover',
        position: 'center'
      })
      .png()
      .toBuffer();

    // Check if we have text to render
    const hasText = (slide.title && slide.title.trim().length > 0) || 
                    (slide.subtitle && slide.subtitle.trim().length > 0);
    
    let finalImage;
    if (hasText) {
      // Create text overlay with custom parameters support
      const textSVG = createTextSVG({
        title: slide.title || '',
        subtitle: slide.subtitle || '',
        textColor: slide.textColor || '#FFFFFF',
        fontFamily: slide.fontFamily || 'Arial',
        width,
        height,
        titleSize: slide.titleSize,
        subtitleSize: slide.subtitleSize,
        titleX: slide.titleX,
        titleY: slide.titleY,
        subtitleX: slide.subtitleX,
        subtitleY: slide.subtitleY,
        maxTitleWidth: slide.maxTitleWidth,
        maxSubtitleWidth: slide.maxSubtitleWidth,
        textAlign: slide.textAlign || 'center'
      });

      // Composite text over background
      finalImage = await sharp(processedBackground)
        .composite([{
          input: textSVG,
          top: 0,
          left: 0
        }])
        .png()
        .toBuffer();
    } else {
      // No text, just use the background
      finalImage = processedBackground;
    }

    // Convert to base64
    const base64 = finalImage.toString('base64');

    return {
      base64: `data:image/png;base64,${base64}`,
      filename: `carousel-slide-${index + 1}.png`,
      success: true
    };
  } catch (error) {
    console.error(`Error generating slide ${index + 1}:`, error.message);
    return {
      success: false,
      error: error.message,
      filename: `carousel-slide-${index + 1}.png`
    };
  }
}

/**
 * Upload to Google Drive (optional)
 */
async function uploadToDrive({ base64, filename, accessToken, folderId }) {
  const { google } = require('googleapis');

  try {
    const drive = google.drive({ version: 'v3' });

    // Remove data:image/png;base64, prefix
    const base64Data = base64.replace(/^data:image\/\w+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');

    const fileMetadata = {
      name: filename,
      parents: folderId ? [folderId] : []
    };

    const media = {
      mimeType: 'image/png',
      body: require('stream').Readable.from(buffer)
    };

    const response = await drive.files.create({
      resource: fileMetadata,
      media: media,
      fields: 'id, webViewLink, webContentLink',
      auth: new google.auth.OAuth2().setCredentials({
        access_token: accessToken
      })
    });

    return {
      success: true,
      fileId: response.data.id,
      webViewLink: response.data.webViewLink,
      webContentLink: response.data.webContentLink
    };
  } catch (error) {
    console.error(`Error uploading ${filename} to Drive:`, error.message);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Main handler
 */
exports.handler = async (event, context) => {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      },
      body: ''
    };
  }

  // Only allow POST
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: 'Method not allowed. Use POST.' })
    };
  }

  try {
    // Rate limiting
    const clientIP = event.headers['x-forwarded-for'] ||
                    event.headers['client-ip'] ||
                    'unknown';

    const requestCount = rateLimitCache.get(clientIP) || 0;
    if (requestCount >= RATE_LIMIT) {
      return {
        statusCode: 429,
        body: JSON.stringify({
          error: 'Rate limit exceeded. Maximum 10 requests per minute.'
        })
      };
    }
    rateLimitCache.set(clientIP, requestCount + 1);

    // Parse request body
    const body = JSON.parse(event.body);
    const {
      backgrounds = [],
      slides = [],
      width = 1080,
      height = 1080,
      uploadToDrive: shouldUploadToDrive = false,
      driveToken = null,
      driveFolderId = null,
      returnUrls = false // If true, return Drive URLs instead of base64 (reduces payload size)
    } = body;

    // Validation
    if (!Array.isArray(backgrounds) || backgrounds.length === 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'backgrounds array is required and must not be empty' })
      };
    }

    if (!Array.isArray(slides) || slides.length === 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'slides array is required and must not be empty' })
      };
    }

    if (backgrounds.length !== slides.length) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: 'backgrounds and slides arrays must have the same length'
        })
      };
    }

    // Validate dimensions
    if (width < 200 || width > 4000 || height < 200 || height > 4000) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: 'Width and height must be between 200 and 4000 pixels'
        })
      };
    }

    // Generate all slides in parallel
    const startTime = Date.now();
    const slidePromises = backgrounds.map((background, index) =>
      generateSlide({
        background,
        slide: slides[index],
        width,
        height,
        index
      })
    );

    const results = await Promise.all(slidePromises);
    const generationTime = Date.now() - startTime;

    // Separate successful and failed slides
    const successfulSlides = results.filter(r => r.success);
    const failedSlides = results.filter(r => !r.success);

    // Upload to Google Drive if requested
    let driveUrls = [];
    if (shouldUploadToDrive && driveToken && successfulSlides.length > 0) {
      const uploadPromises = successfulSlides.map(slide =>
        uploadToDrive({
          base64: slide.base64,
          filename: slide.filename,
          accessToken: driveToken,
          folderId: driveFolderId
        })
      );
      driveUrls = await Promise.all(uploadPromises);
    }

    // Prepare response images
    // If returnUrls is true and we have Drive URLs, return minimal image data with URLs
    // Otherwise, return full base64 data (but check size limit)
    let responseImages;
    if (returnUrls && driveUrls.length > 0 && driveUrls.every(url => url.success)) {
      // Return minimal data with Drive URLs
      responseImages = successfulSlides.map((slide, index) => ({
        filename: slide.filename,
        success: true,
        driveUrl: driveUrls[index]?.webContentLink || driveUrls[index]?.webViewLink
      }));
    } else {
      // Return full base64 data
      responseImages = successfulSlides;
    }

    // Build response
    const response = {
      success: true,
      images: responseImages,
      failed: failedSlides.length > 0 ? failedSlides : undefined,
      driveUrls: driveUrls.length > 0 ? driveUrls : undefined,
      stats: {
        totalSlides: backgrounds.length,
        successful: successfulSlides.length,
        failed: failedSlides.length,
        generationTimeMs: generationTime,
        dimensions: { width, height }
      }
    };

    // Check response size (Netlify limit is ~6MB)
    const responseString = JSON.stringify(response);
    const responseSizeBytes = Buffer.byteLength(responseString, 'utf8');
    const maxSizeBytes = 6000000; // 6MB limit

    if (responseSizeBytes > maxSizeBytes) {
      // If response is too large and we have Drive URLs, suggest using returnUrls
      if (driveUrls.length > 0) {
        return {
          statusCode: 413,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
          },
          body: JSON.stringify({
            success: false,
            error: `Response payload too large (${Math.round(responseSizeBytes / 1024 / 1024 * 100) / 100}MB). Maximum allowed: 6MB.`,
            suggestion: 'Set "returnUrls": true in your request to return Drive URLs instead of base64 data, or reduce the number of slides.',
            stats: {
              totalSlides: backgrounds.length,
              successful: successfulSlides.length,
              failed: failedSlides.length,
              generationTimeMs: generationTime,
              dimensions: { width, height }
            }
          })
        };
      } else {
        // No Drive URLs available, suggest reducing slides or enabling Drive upload
        return {
          statusCode: 413,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
          },
          body: JSON.stringify({
            success: false,
            error: `Response payload too large (${Math.round(responseSizeBytes / 1024 / 1024 * 100) / 100}MB). Maximum allowed: 6MB.`,
            suggestion: 'Reduce the number of slides, use smaller dimensions, or enable Google Drive upload with "returnUrls": true to get URLs instead of base64 data.',
            stats: {
              totalSlides: backgrounds.length,
              successful: successfulSlides.length,
              failed: failedSlides.length,
              generationTimeMs: generationTime,
              dimensions: { width, height }
            }
          })
        };
      }
    }

    // Return response
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: responseString
    };

  } catch (error) {
    console.error('Error in carousel handler:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({
        success: false,
        error: error.message || 'Internal server error',
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      })
    };
  }
};
