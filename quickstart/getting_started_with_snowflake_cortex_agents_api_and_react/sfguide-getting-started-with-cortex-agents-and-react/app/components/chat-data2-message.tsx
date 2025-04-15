import { cx } from 'class-variance-authority';
import { motion } from 'framer-motion';
import { LoaderIcon } from './icons';

export const Data2AnalyticsMessage = ({ message }: { message: string }) => {
    return (
        <motion.div
            className="w-full mx-auto max-w-3xl pr-4 pl-0 group/message"
            initial={{ y: 5, opacity: 0 }}
            animate={{ y: 0, opacity: 1, transition: { delay: 0.25 } }}
        >
            <div
                className={cx(
                    'h-[24px] overflow-hidden items-center flex gap-0.5 group-data-[role:user]/message:px-3 w-full group-data-[role:user]/message:w-fit group-data-[role:user]/message:ml-auto group-data-[role:user]/message:max-w-2xl group-data-[role:user]/message:py-2 rounded-xl',
                    {
                        'group-data-[role:user]/message:bg-muted': true,
                    }
                )}
            >
                <motion.div
                    className="size-6 flex items-center rounded-full justify-center shrink-0 border-none"
                    animate={{ rotate: 360 }}
                    transition={{ repeat: Infinity, duration: 1, ease: "linear" }}
                >
                    <LoaderIcon size={12} />
                </motion.div>

                <div className="flex flex-col gap-2 w-full">
                    <div className="flex flex-col gap-4 text-muted-foreground text-sm">
                        {message}
                    </div>
                </div>
            </div>
        </motion.div>
    );
}
