import NextAuth, { NextAuthOptions } from "next-auth"
import GoogleProvider from "next-auth/providers/google"
import GithubProvider from "next-auth/providers/github"
import CredentialsProvider from "next-auth/providers/credentials";
import { NextApiRequest, NextApiResponse } from "next"
import fetch from "node-fetch"

// this will go away once we have a better way to get the Solr host
const SOLR_HOST = process.env.SOLR_HOST || "http://localhost:8983";

// For more information on each option (and a full list of options) go to
// https://next-auth.js.org/configuration/options


export const authOptions: NextAuthOptions = {
  // https://next-auth.js.org/configuration/providers/oauth
  providers: [
    /* EmailProvider({
         server: process.env.EMAIL_SERVER,
         from: process.env.EMAIL_FROM,
       }),
    // Temporarily removing the Apple provider from the demo site as the
    // callback URL for it needs updating due to Vercel changing domains

    Providers.Apple({
      clientId: process.env.APPLE_ID,
      clientSecret: {
        appleId: process.env.APPLE_ID,
        teamId: process.env.APPLE_TEAM_ID,
        privateKey: process.env.APPLE_PRIVATE_KEY,
        keyId: process.env.APPLE_KEY_ID,
      },
    }),
    */
    CredentialsProvider({
      // The name to display on the sign in form (e.g. "Sign in with...")
      name: "Credentials",
      // `credentials` is used to generate a form on the sign in page.
      // You can specify which fields should be submitted, by adding keys to the `credentials` object.
      // e.g. domain, username, password, 2FA token, etc.
      // You can pass any HTML attribute to the <input> tag through the object.
      credentials: {
        username: { label: "Username", type: "text", placeholder: "admin" },
        password: { label: "Password", type: "password" }
      },
      async authorize(credentials, req) {
        if (!credentials || !credentials.username || !credentials.password) {
          return null;
        }
        try {
          const authString = "Basic " + Buffer.from(credentials.username + ":" + credentials.password).toString("base64");
          const solrResponse = await fetch(`${SOLR_HOST}/solr/admin/authentication`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": authString,
              "Access-Control-Allow-Origin": "*",
            },
            body: JSON.stringify({
              "set-property": {
                "forwardCredentials": true,
              },
            }),
          });

          if (solrResponse.ok) {
            // Replace the following line with the actual user object returned by Solr, if applicable
            const user = {
              id: "1",
              name: credentials.username,
              solrAuth: authString,
              solrResponse: {
                status: solrResponse.headers.get("status"),
              }
            }

            if (user) {
              return user;
            } else {
              return null;
            }
          } else {
            return null;
          }
        } catch (error) {
          return null;
        }
      },
    }),
  ],
  theme: {
    colorScheme: "light",
  },
  callbacks: {
    async jwt({ token }) {
      token.userRole = "admin"
      return token
    },
  },
}

export default NextAuth(authOptions)
