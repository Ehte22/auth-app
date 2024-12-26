import express from "express"
import * as authController from "../controllers/auth.controller"
import { protectedRoute, restrict } from "matic-ui"

const AUTH_ROUTER = express.Router()

AUTH_ROUTER
    .post("/sign-up", authController.signUp)
    .post("/sign-in", authController.signIn)
    .post("/sign-out", authController.signOut)
    .get("/get-users", protectedRoute, authController.getAllUsers)

export default AUTH_ROUTER