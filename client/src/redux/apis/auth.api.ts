import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react"
import { IUser } from "../../models/user.interface"

export const authApi = createApi({
    reducerPath: "authApi",
    // baseQuery: fetchBaseQuery({ baseUrl: `${process.env.REACT_APP_BASE_URL}/api/v1/auth`, credentials: "include" }),
    baseQuery: fetchBaseQuery({ baseUrl: `https://auth-app-one-nu.vercel.app/api/v1/auth`, credentials: "include" }),
    tagTypes: ["auth"],
    endpoints: (builder) => {
        return {
            signUp: builder.mutation<{ message: string, result: IUser }, IUser>({
                query: userData => {
                    return {
                        url: "/sign-up",
                        method: "POST",
                        body: userData
                    }
                },
                transformResponse: (data: { message: string, result: IUser }) => {
                    return data
                },
                transformErrorResponse: (error: { status: number, data: { message: string } }) => {
                    return error.data.message
                }
            }),

            signIn: builder.mutation<{ message: string, result: IUser }, { username: string, password: string }>({
                query: userData => {
                    return {
                        url: "/sign-in",
                        method: "POST",
                        body: userData
                    }
                },
                transformResponse: (data: { message: string, result: IUser }) => {
                    localStorage.setItem("user", JSON.stringify(data.result))
                    return data
                },
                transformErrorResponse: (error: { status: number, data: { message: string } }) => {
                    return error.data.message
                }
            }),

            signOut: builder.mutation<string, void>({
                query: () => {
                    return {
                        url: "/sign-out",
                        method: "POST",
                    }
                },
                transformResponse: (data: { message: string }) => {
                    localStorage.removeItem("user")
                    return data.message
                }
            }),

        }
    }
})

export const {
    useSignUpMutation,
    useSignInMutation,
    useSignOutMutation,
} = authApi
