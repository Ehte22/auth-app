import { Request, Response } from "express"
import asyncHandler from "express-async-handler"
import bcrypt from "bcrypt"
import dotenv from "dotenv"
import { User } from "../models/User"
import { generateToken } from "../utils/generateToken"

dotenv.config()

export const signUp = asyncHandler(async (req: Request, res: Response): Promise<any> => {

    const { email, password } = req.body

    const user = await User.findOne({ email })
    if (user) {
        return res.status(400).json({ message: "Email already exist" })
    }

    const hashPassword = await bcrypt.hash(password, 10)
    const result = await User.create({ ...req.body, password: hashPassword })

    res.status(200).json({
        message: "User SignUp Success", result: {
            _id: result._id,
            name: result.name,
            email: result.email,
            phone: result.phone,
            role: result.role
        }
    })

})

export const signIn = asyncHandler(async (req: Request, res: Response): Promise<any> => {
    const { username, password } = req.body

    const result = await User.findOne({
        $or: [
            { email: username },
            { phone: username },
        ]
    })

    if (!result) {
        return res.status(400).json({ message: "Invalid Credential - Username do not match" })
    }

    const verify = await bcrypt.compare(password, result.password)
    if (!verify) {
        return res.status(400).json({ message: "Invalid Credential - Password do not match" })
    }

    const token = generateToken({ userId: result._id, role: result.role })

    res.cookie("user", token, {
        maxAge: 864000000,
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "none",
    })

    res.status(200).json({
        message: "User Login Success", result: {
            _id: result._id,
            name: result.name,
            username: result.email,
            role: result.role,
            token
        }
    })
})

export const signOut = asyncHandler(async (req: Request, res: Response): Promise<any> => {
    res.clearCookie("user")
    res.status(200).json({ message: "User Logout Success" })
})

export const getAllUsers = asyncHandler(async (req: Request, res: Response): Promise<any> => {
    const result = await User.find()
    res.status(200).json({ message: "Users Fetch Success", result })
})