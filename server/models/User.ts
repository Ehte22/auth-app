import mongoose, { Model, Schema } from "mongoose"

export interface IUser {
    name: string
    email: string
    phone: string
    password: string
    profile: string
    role: string
}

const userSchema = new Schema<IUser>({
    name: {
        type: String,
        required: true
    },
    email: {
        type: String,
        required: true
    },
    phone: {
        type: String,
        required: true
    },
    password: {
        type: String,
        required: true
    },
    profile: {
        type: String,
    },
    role: {
        type: String,
        required: true,
        enum: ["user", "admin"],
        default: "user"
    },
}, { timestamps: true })

export const User: Model<IUser> = mongoose.model<IUser>("user", userSchema)