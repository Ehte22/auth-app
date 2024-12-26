import React, { useEffect } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { zodResolver } from "@hookform/resolvers/zod";
import { Link, useNavigate } from "react-router-dom";
import { useSignInMutation } from "../redux/apis/auth.api";
import { toast } from "../services/toast";

const Login: React.FC = () => {
    const [signIn, { data, error, isSuccess, isError }] = useSignInMutation()
    const navigate = useNavigate()

    const loginSchema = z.object({
        username: z
            .string()
            .min(1, "Username is required")
            .email("Invalid email address"),
        password: z.string().min(6, "Password must be at least 6 characters"),
    });

    type LoginFormValues = z.infer<typeof loginSchema>;

    const {
        register,
        handleSubmit,
        formState: { errors },
    } = useForm<LoginFormValues>({
        resolver: zodResolver(loginSchema),
    });

    const onSubmit = (data: LoginFormValues) => {
        signIn(data)
    };

    useEffect(() => {
        if (isSuccess) {
            toast.showSuccess(data.message)
            navigate("/product")
        }

        if (isError) {
            toast.showError(error as string)
        }
    }, [data, error, isSuccess, isError, navigate])


    return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50">
            <div className="w-full max-w-md bg-white p-8 rounded shadow-md">
                <h1 className="text-2xl font-bold text-teal-700 text-center mb-6">Login</h1>
                <form onSubmit={handleSubmit(onSubmit)}>
                    {/* Email Field */}
                    <div className="mb-4">
                        <label htmlFor="email" className="block text-gray-700 font-medium mb-2">
                            Username
                        </label>
                        <input
                            type="email"
                            id="email"
                            className={`w-full p-2 border rounded focus:outline-none focus:ring-2 focus:ring-teal-700 ${errors.username ? "border-red-500" : "border-gray-300"
                                }`}
                            {...register("username")}
                        />
                        <p className="text-red-600 text-sm mt-1">{errors.username?.message}</p>

                    </div>

                    {/* Password Field */}
                    <div className="mb-6">
                        <label htmlFor="password" className="block text-gray-700 font-medium mb-2">
                            Password
                        </label>
                        <input
                            type="password"
                            id="password"
                            className={`w-full p-2 border rounded focus:outline-none focus:ring-2 focus:ring-teal-700 ${errors.password ? "border-red-500" : "border-gray-300"
                                }`}
                            {...register("password")}
                        />
                        {errors.password && (
                            <p className="text-red-600 text-sm mt-1">{errors.password.message}</p>
                        )}
                    </div>

                    {/* Submit Button */}
                    <button
                        type="submit"
                        className="w-full bg-teal-700 text-white py-2 rounded hover:bg-teal-800 focus:outline-none focus:ring-2 focus:ring-teal-700"
                    >
                        Login
                    </button>
                </form>

                {/* Register Link */}
                <p className="text-sm text-gray-600 text-center mt-4">
                    Don't have an account?{' '}
                    <Link to="/auth/sign-up" className="text-teal-700 hover:underline">
                        Register here
                    </Link>
                </p>
            </div>
        </div>
    );
};

export default Login;
