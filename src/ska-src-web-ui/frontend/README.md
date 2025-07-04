# SKA SRC Web UI Frontend

A React-based frontend for the SKA SRC Web UI that provides a user-friendly interface for token management and authentication.

## Features

- **Token Request**: Initiate OIDC device flow authentication
- **Device Flow Display**: Show user codes and verification URIs
- **Automatic Polling**: Monitor authentication completion
- **Token Management**: View and exchange existing tokens
- **Health Monitoring**: Check backend system status

## Prerequisites

- Node.js (version 14 or higher)
- npm or yarn
- Backend server running on port 8000

## Setup

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Start the development server**:
   ```bash
   npm start
   ```

The frontend will start on `http://localhost:3000` and automatically proxy API requests to the backend on `http://localhost:8000`.

## Usage

### Requesting a Token

1. Click the "Request Token" button
2. The system will initiate the OIDC device flow
3. You'll see a user code and verification URI
4. Click the verification URI to open the IAM authentication page
5. Enter the user code on the IAM page
6. Complete authentication in your browser
7. The frontend will automatically detect completion and obtain the token

### Managing Tokens

- View existing tokens in the "Existing Tokens" section
- Exchange tokens for specific services
- Refresh the token list to see updates

### System Status

- Check backend health using the "Check Backend Health" button
- Monitor system status messages

## Development

The frontend uses:
- **React 18** for the UI framework
- **Axios** for API communication
- **CSS** for styling (no external UI libraries)

### Project Structure

```
src/
├── App.js          # Main application component
├── App.css         # App-specific styles
├── index.js        # React entry point
└── index.css       # Global styles
```

### API Integration

The frontend communicates with the backend API at `/api/v1` and includes:
- Token request and completion
- Token listing and exchange
- Health monitoring

## Building for Production

To create a production build:

```bash
npm run build
```

This creates an optimized build in the `build/` directory.

## Troubleshooting

- **Backend Connection**: Ensure the backend is running on port 8000
- **CORS Issues**: The development server proxies requests to avoid CORS
- **Authentication**: Make sure your IAM configuration is correct 