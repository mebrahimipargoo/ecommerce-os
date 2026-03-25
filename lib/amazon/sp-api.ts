export type AmazonSpApiCredentials = {
  lwaClientId: string;
  lwaClientSecret: string;
  /**
   * LWA scopes vary by use-case. For initial connectivity tests, a minimal SP-API scope
   * can be used. You can override this per call if needed.
   */
  scope?: string;
};

export type AmazonAccessTokenResponse = {
  accessToken: string;
  tokenType: string;
  expiresIn: number;
};

type AmazonTokenSuccess = {
  access_token: string;
  token_type: string;
  expires_in: number;
};

type AmazonTokenError = {
  error?: string;
  error_description?: string;
};

const DEFAULT_LWA_SCOPE = "sellingpartnerapi::notifications";
const AMAZON_OAUTH_TOKEN_URL = "https://api.amazon.com/auth/o2/token";

export async function getAmazonAccessToken(
  credentials: AmazonSpApiCredentials
): Promise<AmazonAccessTokenResponse> {
  const scope = credentials.scope?.trim() || DEFAULT_LWA_SCOPE;

  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: credentials.lwaClientId,
    client_secret: credentials.lwaClientSecret,
    scope,
  });

  const response = await fetch(AMAZON_OAUTH_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    },
    body,
    cache: "no-store",
  });

  const raw = (await response.json().catch(() => null)) as
    | AmazonTokenSuccess
    | AmazonTokenError
    | null;

  if (!response.ok) {
    const errorMessage =
      raw && "error_description" in raw && raw.error_description
        ? raw.error_description
        : "Amazon OAuth token request failed.";
    throw new Error(errorMessage);
  }

  if (!raw || !("access_token" in raw) || typeof raw.access_token !== "string") {
    throw new Error("Amazon OAuth token response was not in the expected format.");
  }

  return {
    accessToken: raw.access_token,
    tokenType: raw.token_type ?? "bearer",
    expiresIn: raw.expires_in ?? 0,
  };
}

export async function fetchAmazonClaims(accessToken: string): Promise<{
  simulated: true;
  message: string;
}> {
  if (!accessToken) {
    throw new Error("Missing access token.");
  }

  // Placeholder: in the next iteration, call SP-API endpoints (Orders/Finances) with
  // SigV4 signing and map results into our unified claims model.
  return {
    simulated: true,
    message: "Simulated Amazon claims fetch (Finances/Orders).",
  };
}

