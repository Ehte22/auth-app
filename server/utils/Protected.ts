import { NextFunction, Request, Response } from "express"

import asyncHandler from "express-async-handler"
import jwt, { VerifyErrors } from "jsonwebtoken"
import dotenv from "dotenv"

dotenv.config()

interface IRequest extends Request {
    user?: {
        userId: string;
        role: string;
        [key: string]: any;
    }
}

export const protectedRoute = asyncHandler(async (req: IRequest, res: Response, next: NextFunction): Promise<any> => {
    console.log(req.cookies.user);

    if (!req.cookies.user) {
        return res.status(404).json({ message: "No cookie found" })
    }

    const JWT_KEY = process.env.JWT_KEY
    if (!JWT_KEY) {
        return res.status(404).json({ message: "JWT key not found" })
    }

    jwt.verify(req.cookies.user, JWT_KEY, (err: VerifyErrors | null, decoded: any) => {
        if (err) {
            if (err.name === 'TokenExpiredError') {
                return res.status(401).json({ message: "Token expired" });
            }
            return res.status(403).json({ message: "JWT error", error: err })
        }
        console.log(decoded);

        req.user = decoded
        req.body.userId = decoded.userId
        next()
    })
})

export const restrict = (role: string) => {
    return (req: IRequest, res: Response, next: NextFunction): any => {
        console.log(role, req.user);

        if (!req.user || req.user.role !== role) {
            return res.status(403).json({ message: "You don't have permission to perform this action" })
        }
        next()
    }
}