import React, { useEffect } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { zodResolver } from "@hookform/resolvers/zod";
import { Link, useNavigate } from "react-router-dom";
import { useSignUpMutation } from "../redux/apis/auth.api";
import { toast } from "../services/toast";

const Register: React.FC = () => {
    const [signUp, { data, error, isSuccess, isError }] = useSignUpMutation()

    const navigate = useNavigate()

    const signUpSchema = z.object({
        name: z
            .string()
            .min(1, "Name is required")
            .max(50, "Name must not exceed 50 characters"),
        email: z
            .string()
            .min(1, "Email is required")
            .email("Invalid email address"),
        phone: z
            .string()
            .min(10, "Phone number must be 10 digits")
            .max(10, "Phone number must be 10 digits")
            .regex(/^\d{10}$/, "Phone number must be numeric"),
        password: z
            .string()
            .min(6, "Password must be at least 6 characters")
            .regex(/^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[@$!%*?&#]).{6,}$/, "Password must contain uppercase, lowercase, number, and special character"),
    });

    type FormValues = z.infer<typeof signUpSchema>;

    const {
        register,
        handleSubmit,
        formState: { errors },
    } = useForm<FormValues>({ resolver: zodResolver(signUpSchema) });

    // Handle form submission
    const onSubmit = (data: FormValues) => {
        signUp(data)
    };

    useEffect(() => {
        if (isSuccess) {
            toast.showSuccess(data.message)
            navigate("/auth")
        }
        if (isError) {
            toast.showError(error as string)
        }
    }, [data, error, isSuccess, isError, navigate])

    return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50">
            <div className="bg-white shadow-md rounded-lg p-8 w-full max-w-md">
                <h2 className="text-2xl font-bold text-teal-700 text-center mb-6">Sign Up</h2>
                <form onSubmit={handleSubmit(onSubmit)}>
                    {/* Name Field */}
                    <div className="mb-4">
                        <label className="block text-gray-700 mb-2">Name</label>
                        <input
                            type="text"
                            className="w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-teal-700"
                            {...register("name")}
                        />
                        {errors.name && <p className="text-red-600 text-sm mt-1">{errors.name.message}</p>}
                    </div>

                    {/* Email Field */}
                    <div className="mb-4">
                        <label className="block text-gray-700 mb-2">Email</label>
                        <input
                            type="text"
                            className="w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-teal-700"
                            {...register("email")}
                        />
                        {errors.email && <p className="text-red-600 text-sm mt-1">{errors.email.message}</p>}
                    </div>

                    {/* Phone Field */}
                    <div className="mb-4">
                        <label className="block text-gray-700 mb-2">Phone</label>
                        <input
                            type="text"
                            className="w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-teal-700"
                            {...register("phone")}
                        />
                        {errors.phone && <p className="text-red-600 text-sm mt-1">{errors.phone.message}</p>}
                    </div>

                    {/* Password Field */}
                    <div className="mb-4">
                        <label className="block text-gray-700 mb-2">Password</label>
                        <input
                            type="password"
                            className="w-full px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-teal-700"
                            {...register("password")}
                        />
                        {errors.password && (
                            <p className="text-red-600 text-sm mt-1">{errors.password.message}</p>
                        )}
                    </div>

                    <button
                        type="submit"
                        className="w-full bg-teal-700 text-white py-2 px-4 rounded-lg hover:bg-teal-800 focus:outline-none focus:ring-2 focus:ring-teal-700"
                    >
                        Sign Up
                    </button>
                </form>

                {/* Login Link */}
                <p className="text-sm text-gray-600 text-center mt-4">
                    Already have an account?{' '}
                    <Link to="/auth" className="text-teal-700 hover:underline">
                        Sign In here
                    </Link>
                </p>
            </div>
        </div>
    );
};

export default Register;
