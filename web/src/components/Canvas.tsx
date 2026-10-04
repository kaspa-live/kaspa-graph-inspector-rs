import React, {useEffect, useRef} from 'react'
import Dag from "../dag/Dag";
import { styled } from '@mui/material/styles';

const StyledCanevas = styled('canvas')({
    padding: 0,
    margin: 0,

    position: 'relative',
    width: '100%',
    height: '100%',
})


const Canvas = ({dag}: { dag: Dag }) => {
    const canvasRef = useRef<HTMLCanvasElement>(null);

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) {
            return;
        }
        dag.initialize(canvas);

        return () => {
            dag.stop();
        };
    }, [dag]);

    return <StyledCanevas ref={canvasRef} />;
}

export default Canvas
