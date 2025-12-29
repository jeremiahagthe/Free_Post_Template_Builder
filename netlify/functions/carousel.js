const sharp = require('sharp');
const https = require('https');
const http = require('http');
const { URL } = require('url');
const NodeCache = require('node-cache');

// Rate limiting cache (10 requests per minute per IP)
const rateLimitCache = new NodeCache({ stdTTL: 60 });
const RATE_LIMIT = 10;

/**
 * Download image from URL
 */
async function downloadImage(url) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const protocol = parsedUrl.protocol === 'https:' ? https : http;

    const request = protocol.get(url, { timeout: 5000 }, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download image: HTTP ${response.statusCode}`));
        return;
      }

      const chunks = [];
      response.on('data', (chunk) => chunks.push(chunk));
      response.on('end', () => resolve(Buffer.concat(chunks)));
      response.on('error', reject);
    });

    request.on('error', reject);
    request.on('timeout', () => {
      request.destroy();
      reject(new Error('Image download timeout'));
    });
  });
}

/**
 * Create text overlay SVG with title and subtitle
 */
function createTextSVG({ title, subtitle, textColor = '#FFFFFF', width, height, fontFamily = 'Arial' }) {
  const titleSize = Math.floor(width * 0.055); // ~60pt for 1080px
  const subtitleSize = Math.floor(width * 0.033); // ~36pt for 1080px
  const padding = Math.floor(width * 0.05);
  const maxTextWidth = width - (padding * 2);

  // Check if we have any text to render
  const hasTitle = title && title.trim().length > 0;
  const hasSubtitle = subtitle && subtitle.trim().length > 0;
  
  // Return empty SVG if no text
  if (!hasTitle && !hasSubtitle) {
    return Buffer.from(`<svg width="${width}" height="${height}"></svg>`);
  }

  // Calculate vertical positioning
  let titleY = height / 2;
  let subtitleY = height / 2;
  
  if (hasTitle && hasSubtitle) {
    titleY = height / 2 - subtitleSize;
    subtitleY = height / 2 + titleSize;
  } else if (hasTitle) {
    titleY = height / 2;
  } else if (hasSubtitle) {
    subtitleY = height / 2;
  }

  // Map font names to font-family CSS strings
  const fontFamilyMap = {
    'Arial': 'Arial, Helvetica, sans-serif',
    'Helvetica': 'Helvetica, Arial, sans-serif',
    'Roboto': 'Roboto, Arial, sans-serif',
    'Open Sans': '"Open Sans", Arial, sans-serif',
    'Montserrat': 'Montserrat, Arial, sans-serif',
    'Bebas Neue': '"Bebas Neue", Impact, Arial, sans-serif',
    'Impact': 'Impact, Arial Black, sans-serif',
    'Futura': 'Futura, "Trebuchet MS", Arial, sans-serif',
    'Georgia': 'Georgia, serif',
    'Times': '"Times New Roman", Times, serif'
  };

  const fontFamilyCSS = fontFamilyMap[fontFamily] || fontFamilyMap['Arial'];

  // Note: Sharp doesn't fetch external resources, so only system fonts will work
  // Google Fonts (Roboto, Montserrat, etc.) will fall back to Arial
  // To use custom fonts, they must be embedded as base64 data URIs

  // Create SVG with text
  const svg = `
    <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <filter id="shadow">
          <feDropShadow dx="2" dy="2" stdDeviation="4" flood-opacity="0.5"/>
        </filter>
        <style>
          .title {
            font-family: ${fontFamilyCSS};
            font-size: ${titleSize}px;
            font-weight: bold;
            fill: ${textColor};
            stroke: rgba(0,0,0,0.3);
            stroke-width: 2;
            paint-order: stroke fill;
            filter: url(#shadow);
          }
          .subtitle {
            font-family: ${fontFamilyCSS};
            font-size: ${subtitleSize}px;
            font-weight: normal;
            fill: ${textColor};
            stroke: rgba(0,0,0,0.2);
            stroke-width: 1;
            paint-order: stroke fill;
            filter: url(#shadow);
          }
        </style>
      </defs>

      ${hasTitle ? `
      <!-- Title -->
      <text x="${width/2}" y="${titleY}"
            text-anchor="middle"
            class="title">
        ${escapeXml(title)}
      </text>
      ` : ''}

      ${hasSubtitle ? `
      <!-- Subtitle -->
      <text x="${width/2}" y="${subtitleY}"
            text-anchor="middle"
            class="subtitle">
        ${escapeXml(subtitle)}
      </text>
      ` : ''}
    </svg>
  `;

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
      // Create text overlay
      const textSVG = createTextSVG({
        title: slide.title || '',
        subtitle: slide.subtitle || '',
        textColor: slide.textColor || '#FFFFFF',
        fontFamily: slide.fontFamily || 'Arial',
        width,
        height
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
      driveFolderId = null
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

    // Return response
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({
        success: true,
        images: successfulSlides,
        failed: failedSlides.length > 0 ? failedSlides : undefined,
        driveUrls: driveUrls.length > 0 ? driveUrls : undefined,
        stats: {
          totalSlides: backgrounds.length,
          successful: successfulSlides.length,
          failed: failedSlides.length,
          generationTimeMs: generationTime,
          dimensions: { width, height }
        }
      })
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
