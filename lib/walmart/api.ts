export type WalmartApiCredentials = {
  clientId: string;
  clientSecret: string;
  /** Marketplace: us, mx, ca, or cl. Default: us */
  market?: "us" | "mx" | "ca" | "cl";
};

export type WalmartAccessTokenResponse = {
  accessToken: string;
  tokenType: string;
  expiresIn: number;
};

type WalmartTokenSuccess = {
  access_token: string;
  token_type: string;
  expires_in: number;
};

type WalmartTokenError = {
  error?: string;
  error_description?: string;
  message?: string;
};

const WALMART_TOKEN_URL = "https://marketplace.walmartapis.com/v3/token";
const WALMART_SANDBOX_TOKEN_URL = "https://sandbox.walmartapis.com/v3/token";

function getTokenUrl(useSandbox: boolean): string {
  return useSandbox ? WALMART_SANDBOX_TOKEN_URL : WALMART_TOKEN_URL;
}

function generateCorrelationId(): string {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export async function getWalmartAccessToken(
  credentials: WalmartApiCredentials,
  options?: { sandbox?: boolean }
): Promise<WalmartAccessTokenResponse> {
  const url = getTokenUrl(options?.sandbox ?? false);

  const basicAuth = Buffer.from(
    `${credentials.clientId}:${credentials.clientSecret}`,
    "utf-8"
  ).toString("base64");

  const body = new URLSearchParams({
    grant_type: "client_credentials",
  });

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${basicAuth}`,
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded",
      "WM_SVC.NAME": "Walmart Marketplace",
      "WM_QOS.CORRELATION_ID": generateCorrelationId(),
    },
    body,
    cache: "no-store",
  });

  const raw = (await response.json().catch(() => null)) as
    | WalmartTokenSuccess
    | WalmartTokenError
    | { clientCredentialsRes?: { value?: WalmartTokenSuccess } }
    | null;

  if (!response.ok) {
    const err = raw as WalmartTokenError | null;
    const errorMessage =
      err && "error_description" in err && err.error_description
        ? err.error_description
        : err && "message" in err && err.message
          ? err.message
          : err && "error" in err && typeof err.error === "string"
            ? err.error
            : `Walmart token request failed (${response.status}).`;
    throw new Error(errorMessage);
  }

  const token =
    raw && "access_token" in raw && typeof (raw as WalmartTokenSuccess).access_token === "string"
      ? (raw as WalmartTokenSuccess)
      : (raw as { clientCredentialsRes?: { value?: WalmartTokenSuccess } })?.clientCredentialsRes
          ?.value;

  if (!token || typeof token.access_token !== "string") {
    throw new Error("Walmart token response was not in the expected format.");
  }

  return {
    accessToken: token.access_token,
    tokenType: token.token_type ?? "bearer",
    expiresIn: token.expires_in ?? 0,
  };
}
